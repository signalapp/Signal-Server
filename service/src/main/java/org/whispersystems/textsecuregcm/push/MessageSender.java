/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.push;

import static com.codahale.metrics.MetricRegistry.name;
import static org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.util.Util;
import java.util.concurrent.CompletableFuture;

/**
 * A MessageSender sends Signal messages to destination devices. Messages may be "normal" user-to-user messages,
 * ephemeral ("online") messages like typing indicators, or delivery receipts.
 * <p/>
 * If a client is not actively connected to a Signal server to receive a message as soon as it is sent, the
 * MessageSender will send a push notification to the destination device if possible. Some messages may be designated
 * for "online" delivery only and will not be delivered (and clients will not be notified) if the destination device
 * isn't actively connected to a Signal server.
 *
 * @see ClientPresenceManager
 * @see org.whispersystems.textsecuregcm.storage.MessageAvailabilityListener
 * @see ReceiptSender
 */
public class MessageSender {

  private final PubSubClientEventManager pubSubClientEventManager;
  private final MessagesManager messagesManager;
  private final PushNotificationManager pushNotificationManager;

  private static final String SEND_COUNTER_NAME = name(MessageSender.class, "sendMessage");
  private static final String CHANNEL_TAG_NAME = "channel";
  private static final String EPHEMERAL_TAG_NAME = "ephemeral";
  private static final String CLIENT_ONLINE_TAG_NAME = "clientOnline";
  private static final String URGENT_TAG_NAME = "urgent";
  private static final String STORY_TAG_NAME = "story";
  private static final String SEALED_SENDER_TAG_NAME = "sealedSender";

  private static final Counter CLIENT_PRESENCE_ERROR =
      Metrics.counter(MetricsUtil.name(MessageSender.class, "clientPresenceError"));

  public MessageSender(final PubSubClientEventManager pubSubClientEventManager,
      final MessagesManager messagesManager,
      final PushNotificationManager pushNotificationManager) {

    this.pubSubClientEventManager = pubSubClientEventManager;
    this.messagesManager = messagesManager;
    this.pushNotificationManager = pushNotificationManager;
  }

  public CompletableFuture<Void> sendMessage(final Account account, final Device device, final Envelope message, final boolean online) {
    messagesManager.insert(account.getUuid(),
        device.getId(),
        online ? message.toBuilder().setEphemeral(true).build() : message);

    return pubSubClientEventManager.handleNewMessageAvailable(account.getIdentifier(IdentityType.ACI), device.getId())
        .exceptionally(throwable -> {
          // It's unlikely that the message insert (synchronous) would succeed and sending a "new message available"
          // event would fail since both things happen in the same cluster, but just in case, we should "fail open" and
          // act as if the client wasn't present if this happens. This is a conservative measure that biases toward
          // sending more push notifications, though again, it shouldn't happen often.
          CLIENT_PRESENCE_ERROR.increment();
          return false;
        })
        .thenApply(clientPresent -> {
          if (!clientPresent && !online) {
            try {
              pushNotificationManager.sendNewMessageNotification(account, device.getId(), message.getUrgent());
            } catch (final NotPushRegisteredException ignored) {
            }
          }

          return clientPresent;
        })
        .whenComplete((clientPresent, throwable) -> Metrics.counter(SEND_COUNTER_NAME,
                CHANNEL_TAG_NAME, getDeliveryChannelName(device),
                EPHEMERAL_TAG_NAME, String.valueOf(online),
                CLIENT_ONLINE_TAG_NAME, String.valueOf(clientPresent),
                URGENT_TAG_NAME, String.valueOf(message.getUrgent()),
                STORY_TAG_NAME, String.valueOf(message.getStory()),
                SEALED_SENDER_TAG_NAME, String.valueOf(!message.hasSourceServiceId()))
            .increment())
        .thenRun(Util.NOOP)
        .toCompletableFuture();
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
