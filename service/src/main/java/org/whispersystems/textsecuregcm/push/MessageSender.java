/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.push;

import static com.codahale.metrics.MetricRegistry.name;
import static org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;

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

  public void sendMessage(final Account account, final Device device, final Envelope message, final boolean online) {
    final boolean destinationPresent = messagesManager.insert(account.getUuid(),
        device.getId(),
        online ? message.toBuilder().setEphemeral(true).build() : message);

    if (!destinationPresent && !online) {
      try {
        pushNotificationManager.sendNewMessageNotification(account, device.getId(), message.getUrgent());
      } catch (final NotPushRegisteredException ignored) {
      }
    }

    Metrics.counter(SEND_COUNTER_NAME,
            CHANNEL_TAG_NAME, getDeliveryChannelName(device),
            EPHEMERAL_TAG_NAME, String.valueOf(online),
            CLIENT_ONLINE_TAG_NAME, String.valueOf(destinationPresent),
            URGENT_TAG_NAME, String.valueOf(message.getUrgent()),
            STORY_TAG_NAME, String.valueOf(message.getStory()),
            SEALED_SENDER_TAG_NAME, String.valueOf(!message.hasSourceServiceId()))
        .increment();
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
