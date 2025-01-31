/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.push;

import static com.codahale.metrics.MetricRegistry.name;
import static org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.util.Util;

/**
 * A MessageSender sends Signal messages to destination devices. Messages may be "normal" user-to-user messages,
 * ephemeral ("online") messages like typing indicators, or delivery receipts.
 * <p/>
 * If a client is not actively connected to a Signal server to receive a message as soon as it is sent, the
 * MessageSender will send a push notification to the destination device if possible. Some messages may be designated
 * for "online" delivery only and will not be delivered (and clients will not be notified) if the destination device
 * isn't actively connected to a Signal server.
 *
 * @see ReceiptSender
 */
public class MessageSender {

  private final MessagesManager messagesManager;
  private final PushNotificationManager pushNotificationManager;

  private static final String SEND_COUNTER_NAME = name(MessageSender.class, "sendMessage");
  private static final String CHANNEL_TAG_NAME = "channel";
  private static final String EPHEMERAL_TAG_NAME = "ephemeral";
  private static final String CLIENT_ONLINE_TAG_NAME = "clientOnline";
  private static final String URGENT_TAG_NAME = "urgent";
  private static final String STORY_TAG_NAME = "story";
  private static final String SEALED_SENDER_TAG_NAME = "sealedSender";

  public MessageSender(final MessagesManager messagesManager, final PushNotificationManager pushNotificationManager) {
    this.messagesManager = messagesManager;
    this.pushNotificationManager = pushNotificationManager;
  }

  /**
   * Sends messages to devices associated with the given destination account. If a destination device has a valid push
   * notification token and does not have an active connection to a Signal server, then this method will also send a
   * push notification to that device to announce the availability of new messages.
   *
   * @param account the account to which to send messages
   * @param messagesByDeviceId a map of device IDs to message payloads
   */
  public void sendMessages(final Account account, final Map<Byte, Envelope> messagesByDeviceId) {
    messagesManager.insert(account.getIdentifier(IdentityType.ACI), messagesByDeviceId)
        .forEach((deviceId, destinationPresent) -> {
          final Envelope message = messagesByDeviceId.get(deviceId);

          if (!destinationPresent && !message.getEphemeral()) {
            try {
              pushNotificationManager.sendNewMessageNotification(account, deviceId, message.getUrgent());
            } catch (final NotPushRegisteredException ignored) {
            }
          }

          Metrics.counter(SEND_COUNTER_NAME,
                  CHANNEL_TAG_NAME, account.getDevice(deviceId).map(MessageSender::getDeliveryChannelName).orElse("unknown"),
                  EPHEMERAL_TAG_NAME, String.valueOf(message.getEphemeral()),
                  CLIENT_ONLINE_TAG_NAME, String.valueOf(destinationPresent),
                  URGENT_TAG_NAME, String.valueOf(message.getUrgent()),
                  STORY_TAG_NAME, String.valueOf(message.getStory()),
                  SEALED_SENDER_TAG_NAME, String.valueOf(!message.hasSourceServiceId()))
              .increment();
        });
  }

  /**
   * Sends messages to a group of recipients. If a destination device has a valid push notification token and does not
   * have an active connection to a Signal server, then this method will also send a push notification to that device to
   * announce the availability of new messages.
   *
   * @param multiRecipientMessage the multi-recipient message to send to the given recipients
   * @param resolvedRecipients a map of recipients to resolved Signal accounts
   * @param clientTimestamp the time at which the sender reports the message was sent
   * @param isStory {@code true} if the message is a story or {@code false otherwise}
   * @param isEphemeral {@code true} if the message should only be delivered to devices with active connections or
   * {@code false otherwise}
   * @param isUrgent {@code true} if the message is urgent or {@code false otherwise}
   *
   * @return a future that completes when all messages have been inserted into delivery queues
   */
  public CompletableFuture<Void> sendMultiRecipientMessage(final SealedSenderMultiRecipientMessage multiRecipientMessage,
      final Map<SealedSenderMultiRecipientMessage.Recipient, Account> resolvedRecipients,
      final long clientTimestamp,
      final boolean isStory,
      final boolean isEphemeral,
      final boolean isUrgent) {

    return messagesManager.insertMultiRecipientMessage(multiRecipientMessage, resolvedRecipients, clientTimestamp,
            isStory, isEphemeral, isUrgent)
        .thenAccept(clientPresenceByAccountAndDevice ->
            clientPresenceByAccountAndDevice.forEach((account, clientPresenceByDeviceId) ->
                clientPresenceByDeviceId.forEach((deviceId, clientPresent) -> {
                  if (!clientPresent && !isEphemeral) {
                    try {
                      pushNotificationManager.sendNewMessageNotification(account, deviceId, isUrgent);
                    } catch (final NotPushRegisteredException ignored) {
                    }
                  }

                  Metrics.counter(SEND_COUNTER_NAME,
                          CHANNEL_TAG_NAME,
                          account.getDevice(deviceId).map(MessageSender::getDeliveryChannelName).orElse("unknown"),
                          EPHEMERAL_TAG_NAME, String.valueOf(isEphemeral),
                          CLIENT_ONLINE_TAG_NAME, String.valueOf(clientPresent),
                          URGENT_TAG_NAME, String.valueOf(isUrgent),
                          STORY_TAG_NAME, String.valueOf(isStory),
                          SEALED_SENDER_TAG_NAME, String.valueOf(true))
                      .increment();
                })))
        .thenRun(Util.NOOP);
  }

  @VisibleForTesting
  static String getDeliveryChannelName(final Device device) {
    if (device.getGcmId() != null) {
      return "gcm";
    } else if (device.getApnId() != null) {
      return "apn";
    } else if (device.getFetchesMessages()) {
      return "websocket";
    } else {
      return "none";
    }
  }
}
