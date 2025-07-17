/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.push;

import static com.codahale.metrics.MetricRegistry.name;
import static org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.util.DataSize;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.signal.libsignal.protocol.util.Pair;
import org.whispersystems.textsecuregcm.controllers.MessageController;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevices;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.MultiRecipientMismatchedDevicesException;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
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

  // Note that these names deliberately reference `MessageController` for metric continuity
  private static final String REJECT_OVERSIZE_MESSAGE_COUNTER_NAME = name(MessageController.class, "rejectOversizeMessage");
  private static final String CONTENT_SIZE_DISTRIBUTION_NAME = MetricsUtil.name(MessageController.class, "messageContentSize");
  private static final String EMPTY_MESSAGE_LIST_COUNTER_NAME = MetricsUtil.name(MessageSender.class, "emptyMessageList");

  private static final String SEND_COUNTER_NAME = name(MessageSender.class, "sendMessage");
  private static final String EPHEMERAL_TAG_NAME = "ephemeral";
  private static final String CLIENT_ONLINE_TAG_NAME = "clientOnline";
  private static final String URGENT_TAG_NAME = "urgent";
  private static final String STORY_TAG_NAME = "story";
  private static final String SEALED_SENDER_TAG_NAME = "sealedSender";
  private static final String MULTI_RECIPIENT_TAG_NAME = "multiRecipient";
  private static final String SYNC_MESSAGE_TAG_NAME = "sync";

  @VisibleForTesting
  public static final int MAX_MESSAGE_SIZE = (int) DataSize.kibibytes(256).toBytes();

  @VisibleForTesting
  static final byte NO_EXCLUDED_DEVICE_ID = -1;

  public MessageSender(final MessagesManager messagesManager, final PushNotificationManager pushNotificationManager) {
    this.messagesManager = messagesManager;
    this.pushNotificationManager = pushNotificationManager;
  }

  /**
   * Sends messages to devices associated with the given destination account. If a destination device has a valid push
   * notification token and does not have an active connection to a Signal server, then this method will also send a
   * push notification to that device to announce the availability of new messages.
   *
   * @param destination the account to which to send messages
   * @param destinationIdentifier the service identifier to which the messages are addressed
   * @param messagesByDeviceId a map of device IDs to message payloads
   * @param registrationIdsByDeviceId a map of device IDs to device registration IDs
   * @param syncMessageSenderDeviceId if the message is a sync message (i.e. a message to other devices linked to the
   *                                  caller's own account), contains the ID of the device that sent the message
   * @param userAgent the User-Agent string for the sender; may be {@code null} if not known
   *
   * @throws MismatchedDevicesException if the given bundle of messages did not include a message for all required
   * devices, contained messages for devices not linked to the destination account, or devices with outdated
   * registration IDs
   * @throws MessageTooLargeException if the given message payload is too large
   */
  public void sendMessages(final Account destination,
      final ServiceIdentifier destinationIdentifier,
      final Map<Byte, Envelope> messagesByDeviceId,
      final Map<Byte, Integer> registrationIdsByDeviceId,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<Byte> syncMessageSenderDeviceId,
      @Nullable final String userAgent) throws MismatchedDevicesException, MessageTooLargeException {

    final Tag platformTag = UserAgentTagUtil.getPlatformTag(userAgent);

    validateIndividualMessageBundle(destination,
        destinationIdentifier,
        messagesByDeviceId,
        registrationIdsByDeviceId,
        syncMessageSenderDeviceId,
        platformTag);

    messagesManager.insert(destination.getIdentifier(IdentityType.ACI), messagesByDeviceId)
        .forEach((deviceId, destinationPresent) -> {
          final Envelope message = messagesByDeviceId.get(deviceId);

          if (!destinationPresent && !message.getEphemeral()) {
            try {
              pushNotificationManager.sendNewMessageNotification(destination, deviceId, message.getUrgent());
            } catch (final NotPushRegisteredException ignored) {
            }
          }

          final Tags tags = Tags.of(
                  EPHEMERAL_TAG_NAME, String.valueOf(message.getEphemeral()),
                  CLIENT_ONLINE_TAG_NAME, String.valueOf(destinationPresent),
                  URGENT_TAG_NAME, String.valueOf(message.getUrgent()),
                  STORY_TAG_NAME, String.valueOf(message.getStory()),
                  SEALED_SENDER_TAG_NAME, String.valueOf(!message.hasSourceServiceId()),
                  SYNC_MESSAGE_TAG_NAME, String.valueOf(syncMessageSenderDeviceId.isPresent()),
                  MULTI_RECIPIENT_TAG_NAME, "false")
              .and(platformTag);

          Metrics.counter(SEND_COUNTER_NAME, tags).increment();
        });
  }

  /**
   * Sends messages to a group of recipients. If a destination device has a valid push notification token and does not
   * have an active connection to a Signal server, then this method will also send a push notification to that device to
   * announce the availability of new messages.
   * <p>
   * This method sends messages to all <em>resolved</em> recipients. In some cases, a caller may not be able to resolve
   * all recipients to active accounts, but may still choose to send the message. Callers are responsible for rejecting
   * the message if they require full resolution of all recipients, but some recipients could not be resolved.
   *
   * @param multiRecipientMessage the multi-recipient message to send to the given recipients
   * @param resolvedRecipients a map of recipients to resolved Signal accounts
   * @param clientTimestamp the time at which the sender reports the message was sent
   * @param isStory {@code true} if the message is a story or {@code false otherwise}
   * @param isEphemeral {@code true} if the message should only be delivered to devices with active connections or
   * {@code false otherwise}
   * @param isUrgent {@code true} if the message is urgent or {@code false otherwise}
   * @param userAgent the User-Agent string for the sender; may be {@code null} if not known
   *
   * @return a future that completes when all messages have been inserted into delivery queues
   *
   * @throws MultiRecipientMismatchedDevicesException if the given multi-recipient message had did not have all required
   * recipient devices for a recipient account, contained recipients for devices not linked to a destination account, or
   * recipient devices with outdated registration IDs
   * @throws MessageTooLargeException if the given message payload is too large
   */
  public CompletableFuture<Void> sendMultiRecipientMessage(final SealedSenderMultiRecipientMessage multiRecipientMessage,
      final Map<SealedSenderMultiRecipientMessage.Recipient, Account> resolvedRecipients,
      final long clientTimestamp,
      final boolean isStory,
      final boolean isEphemeral,
      final boolean isUrgent,
      @Nullable final String userAgent) throws MultiRecipientMismatchedDevicesException, MessageTooLargeException {

    final Tag platformTag = UserAgentTagUtil.getPlatformTag(userAgent);

    validateMultiRecipientMessageContentLength(multiRecipientMessage, isStory, platformTag);

    final Map<ServiceIdentifier, MismatchedDevices> mismatchedDevicesByServiceIdentifier = new HashMap<>();

    multiRecipientMessage.getRecipients().forEach((serviceId, recipient) -> {
      if (!resolvedRecipients.containsKey(recipient)) {
        // Callers are responsible for rejecting messages if they're missing recipients in a problematic way. If we run
        // into an unresolved recipient here, just skip it.
        return;
      }

      final Account account = resolvedRecipients.get(recipient);
      final ServiceIdentifier serviceIdentifier = ServiceIdentifier.fromLibsignal(serviceId);

      final Map<Byte, Integer> registrationIdsByDeviceId = recipient.getDevicesAndRegistrationIds()
          .collect(Collectors.toMap(Pair::first, pair -> (int) pair.second()));

      getMismatchedDevices(account, serviceIdentifier, registrationIdsByDeviceId, NO_EXCLUDED_DEVICE_ID)
          .ifPresent(mismatchedDevices ->
              mismatchedDevicesByServiceIdentifier.put(serviceIdentifier, mismatchedDevices));
    });

    if (!mismatchedDevicesByServiceIdentifier.isEmpty()) {
      throw new MultiRecipientMismatchedDevicesException(mismatchedDevicesByServiceIdentifier);
    }

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

                  final Tags tags = Tags.of(
                          EPHEMERAL_TAG_NAME, String.valueOf(isEphemeral),
                          CLIENT_ONLINE_TAG_NAME, String.valueOf(clientPresent),
                          URGENT_TAG_NAME, String.valueOf(isUrgent),
                          STORY_TAG_NAME, String.valueOf(isStory),
                          SEALED_SENDER_TAG_NAME, "true",
                          SYNC_MESSAGE_TAG_NAME, "false",
                          MULTI_RECIPIENT_TAG_NAME, "true")
                      .and(platformTag);

                  Metrics.counter(SEND_COUNTER_NAME, tags).increment();
                })))
        .thenRun(Util.NOOP);
  }

  /**
   * Validates that a bundle of messages destined for an individual account is well-formed and may be delivered. Note
   * that all checks performed by this method are also performed by
   * {@link #sendMessages(Account, ServiceIdentifier, Map, Map, Optional, String)}; callers should only invoke this
   * method if they need to verify that a bundle of individual messages is valid <em>before</em> trying to send the
   * messages (i.e. if the caller must take some other action in conjunction with sending the messages and cannot
   * reverse that action if message sending fails).
   *
   * @param destination the account to which to send messages
   * @param destinationIdentifier the service identifier to which the messages are addressed
   * @param messagesByDeviceId a map of device IDs to message payloads
   * @param registrationIdsByDeviceId a map of device IDs to device registration IDs
   * @param syncMessageSenderDeviceId if the message is a sync message (i.e. a message to other devices linked to the
   *                                  caller's own account), contains the ID of the device that sent the message
   * @param userAgent the User-Agent string for the sender; may be {@code null} if not known
   *
   * @throws MismatchedDevicesException if the given bundle of messages did not include a message for all required
   * devices, contained messages for devices not linked to the destination account, or devices with outdated
   * registration IDs
   * @throws MessageTooLargeException if the given message payload is too large
   */
  public static void validateIndividualMessageBundle(final Account destination,
      final ServiceIdentifier destinationIdentifier,
      final Map<Byte, Envelope> messagesByDeviceId,
      final Map<Byte, Integer> registrationIdsByDeviceId,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<Byte> syncMessageSenderDeviceId,
      @Nullable final String userAgent) throws MessageTooLargeException, MismatchedDevicesException {

    validateIndividualMessageBundle(destination,
        destinationIdentifier,
        messagesByDeviceId,
        registrationIdsByDeviceId,
        syncMessageSenderDeviceId,
        UserAgentTagUtil.getPlatformTag(userAgent));
  }

  private static void validateIndividualMessageBundle(final Account destination,
      final ServiceIdentifier destinationIdentifier,
      final Map<Byte, Envelope> messagesByDeviceId,
      final Map<Byte, Integer> registrationIdsByDeviceId,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType") final Optional<Byte> syncMessageSenderDeviceId,
      final Tag platformTag) throws MismatchedDevicesException, MessageTooLargeException {

    if (!destination.isIdentifiedBy(destinationIdentifier)) {
      throw new IllegalArgumentException("Destination account not identified by destination service identifier");
    }

    if (messagesByDeviceId.isEmpty()) {
      Metrics.counter(EMPTY_MESSAGE_LIST_COUNTER_NAME,
          Tags.of(SYNC_MESSAGE_TAG_NAME, String.valueOf(syncMessageSenderDeviceId.isPresent())).and(platformTag)).increment();
    }

    final byte excludedDeviceId;
    if (syncMessageSenderDeviceId.isPresent()) {
      if (messagesByDeviceId.values().stream().anyMatch(message -> StringUtils.isBlank(message.getSourceServiceId()) ||
          !destination.isIdentifiedBy(ServiceIdentifier.valueOf(message.getSourceServiceId())))) {

        throw new IllegalArgumentException("Sync message sender device ID specified, but one or more messages are not addressed to sender");
      }
      excludedDeviceId = syncMessageSenderDeviceId.get();
    } else {
      if (messagesByDeviceId.values().stream().anyMatch(message -> StringUtils.isNotBlank(message.getSourceServiceId()) &&
          destination.isIdentifiedBy(ServiceIdentifier.valueOf(message.getSourceServiceId())))) {

        throw new IllegalArgumentException("Sync message sender device ID not specified, but one or more messages are addressed to sender");
      }
      excludedDeviceId = NO_EXCLUDED_DEVICE_ID;
    }

    final Optional<MismatchedDevices> maybeMismatchedDevices = getMismatchedDevices(destination,
        destinationIdentifier,
        registrationIdsByDeviceId,
        excludedDeviceId);

    if (maybeMismatchedDevices.isPresent()) {
      throw new MismatchedDevicesException(maybeMismatchedDevices.get());
    }

    validateIndividualMessageContentLength(messagesByDeviceId.values(), syncMessageSenderDeviceId.isPresent(), platformTag);
  }

  @VisibleForTesting
  static void validateContentLength(final int contentLength,
      final boolean isMultiRecipientMessage,
      final boolean isSyncMessage,
      final boolean isStory,
      final Tag platformTag) throws MessageTooLargeException {

    final boolean oversize = contentLength > MAX_MESSAGE_SIZE;

    DistributionSummary.builder(CONTENT_SIZE_DISTRIBUTION_NAME)
        .tags(Tags.of(platformTag,
            Tag.of("oversize", String.valueOf(oversize)),
            Tag.of("multiRecipientMessage", String.valueOf(isMultiRecipientMessage)),
            Tag.of("syncMessage", String.valueOf(isSyncMessage)),
            Tag.of("story", String.valueOf(isStory))))
        .publishPercentileHistogram(true)
        .register(Metrics.globalRegistry)
        .record(contentLength);

    if (oversize) {
      Metrics.counter(REJECT_OVERSIZE_MESSAGE_COUNTER_NAME, Tags.of(platformTag,
              Tag.of("multiRecipientMessage", String.valueOf(isMultiRecipientMessage)),
              Tag.of("syncMessage", String.valueOf(isSyncMessage)),
              Tag.of("story", String.valueOf(isStory))))
          .increment();

      throw new MessageTooLargeException();
    }
  }

  @VisibleForTesting
  static Optional<MismatchedDevices> getMismatchedDevices(final Account account,
      final ServiceIdentifier serviceIdentifier,
      final Map<Byte, Integer> registrationIdsByDeviceId,
      final byte excludedDeviceId) {

    final Set<Byte> accountDeviceIds = account.getDevices().stream()
        .map(Device::getId)
        .filter(deviceId -> deviceId != excludedDeviceId)
        .collect(Collectors.toSet());

    final Set<Byte> missingDeviceIds = new HashSet<>(accountDeviceIds);
    missingDeviceIds.removeAll(registrationIdsByDeviceId.keySet());

    final Set<Byte> extraDeviceIds = new HashSet<>(registrationIdsByDeviceId.keySet());
    extraDeviceIds.removeAll(accountDeviceIds);

    final Set<Byte> staleDeviceIds = registrationIdsByDeviceId.entrySet().stream()
        // Filter out device IDs that aren't associated with the given account
        .filter(entry -> !extraDeviceIds.contains(entry.getKey()))
        .filter(entry -> {
          final byte deviceId = entry.getKey();
          final int registrationId = entry.getValue();

          // We know the device must be present because we've already filtered out device IDs that aren't associated
          // with the given account
          final Device device = account.getDevice(deviceId).orElseThrow();
          final int expectedRegistrationId = device.getRegistrationId(serviceIdentifier.identityType());

          return registrationId != expectedRegistrationId;
        })
        .map(Map.Entry::getKey)
        .collect(Collectors.toSet());

    return (!missingDeviceIds.isEmpty() || !extraDeviceIds.isEmpty() || !staleDeviceIds.isEmpty())
        ? Optional.of(new MismatchedDevices(missingDeviceIds, extraDeviceIds, staleDeviceIds))
        : Optional.empty();
  }

  private static void validateIndividualMessageContentLength(final Iterable<Envelope> messages,
      final boolean isSyncMessage,
      final Tag platformTag) throws MessageTooLargeException {

    for (final Envelope message : messages) {
      MessageSender.validateContentLength(message.getContent().size(),
          false,
          isSyncMessage,
          message.getStory(),
          platformTag);
    }
  }

  private static void validateMultiRecipientMessageContentLength(final SealedSenderMultiRecipientMessage multiRecipientMessage,
      final boolean isStory,
      final Tag platformTag) throws MessageTooLargeException {

    for (final SealedSenderMultiRecipientMessage.Recipient recipient : multiRecipientMessage.getRecipients().values()) {
      MessageSender.validateContentLength(multiRecipientMessage.messageSizeForRecipient(recipient),
          true,
          false,
          isStory,
          platformTag);
    }
  }
}
