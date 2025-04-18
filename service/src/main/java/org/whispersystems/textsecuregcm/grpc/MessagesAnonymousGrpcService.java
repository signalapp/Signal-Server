/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.signal.chat.messages.IndividualRecipientMessageBundle;
import org.signal.chat.messages.MultiRecipientMismatchedDevices;
import org.signal.chat.messages.SendMessageResponse;
import org.signal.chat.messages.SendMultiRecipientMessageRequest;
import org.signal.chat.messages.SendMultiRecipientMessageResponse;
import org.signal.chat.messages.SendMultiRecipientStoryRequest;
import org.signal.chat.messages.SendSealedSenderMessageRequest;
import org.signal.chat.messages.SendStoryMessageRequest;
import org.signal.chat.messages.SimpleMessagesAnonymousGrpc;
import org.signal.libsignal.protocol.InvalidMessageException;
import org.signal.libsignal.protocol.InvalidVersionException;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.controllers.MultiRecipientMismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.CardinalityEstimator;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.MessageTooLargeException;
import org.whispersystems.textsecuregcm.push.MessageUtil;
import org.whispersystems.textsecuregcm.spam.GrpcResponse;
import org.whispersystems.textsecuregcm.spam.MessageType;
import org.whispersystems.textsecuregcm.spam.SpamCheckResult;
import org.whispersystems.textsecuregcm.spam.SpamChecker;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;

public class MessagesAnonymousGrpcService extends SimpleMessagesAnonymousGrpc.MessagesAnonymousImplBase {

  private final AccountsManager accountsManager;
  private final RateLimiters rateLimiters;
  private final MessageSender messageSender;
  private final GroupSendTokenUtil groupSendTokenUtil;
  private final CardinalityEstimator messageByteLimitEstimator;
  private final SpamChecker spamChecker;
  private final Clock clock;

  private static final SendMessageResponse SEND_MESSAGE_SUCCESS_RESPONSE = SendMessageResponse.newBuilder().build();

  public MessagesAnonymousGrpcService(final AccountsManager accountsManager,
      final RateLimiters rateLimiters,
      final MessageSender messageSender,
      final GroupSendTokenUtil groupSendTokenUtil,
      final CardinalityEstimator messageByteLimitEstimator,
      final SpamChecker spamChecker,
      final Clock clock) {

    this.accountsManager = accountsManager;
    this.rateLimiters = rateLimiters;
    this.messageSender = messageSender;
    this.messageByteLimitEstimator = messageByteLimitEstimator;
    this.spamChecker = spamChecker;
    this.clock = clock;
    this.groupSendTokenUtil = groupSendTokenUtil;
  }

  @Override
  public SendMessageResponse sendSingleRecipientMessage(final SendSealedSenderMessageRequest request)
      throws StatusException, RateLimitExceededException {

    final ServiceIdentifier destinationServiceIdentifier =
        ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getDestination());

    final Account destination = accountsManager.getByServiceIdentifier(destinationServiceIdentifier)
        .orElseThrow(Status.UNAUTHENTICATED::asException);

    switch (request.getAuthorizationCase()) {
      case UNIDENTIFIED_ACCESS_KEY -> {
        if (!UnidentifiedAccessUtil.checkUnidentifiedAccess(destination, request.getUnidentifiedAccessKey().toByteArray())) {
          throw Status.UNAUTHENTICATED.asException();
        }
      }
      case GROUP_SEND_TOKEN ->
          groupSendTokenUtil.checkGroupSendToken(request.getGroupSendToken(), destinationServiceIdentifier);

      case AUTHORIZATION_NOT_SET -> throw Status.UNAUTHENTICATED.asException();
    }

    return sendIndividualMessage(destination,
        destinationServiceIdentifier,
        request.getMessages(),
        request.getEphemeral(),
        request.getUrgent(),
        false);
  }

  @Override
  public SendMessageResponse sendStory(final SendStoryMessageRequest request)
      throws StatusException, RateLimitExceededException {

    final ServiceIdentifier destinationServiceIdentifier =
        ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getDestination());

    final Optional<Account> maybeDestination = accountsManager.getByServiceIdentifier(destinationServiceIdentifier);

    if (maybeDestination.isEmpty()) {
      // Don't reveal to unauthenticated callers whether a destination account actually exists
      return SEND_MESSAGE_SUCCESS_RESPONSE;
    }

    final Account destination = maybeDestination.get();

    rateLimiters.getStoriesLimiter().validate(destination.getIdentifier(IdentityType.ACI));

    return sendIndividualMessage(destination,
        destinationServiceIdentifier,
        request.getMessages(),
        false,
        request.getUrgent(),
        true);
  }

  private SendMessageResponse sendIndividualMessage(final Account destination,
      final ServiceIdentifier destinationServiceIdentifier,
      final IndividualRecipientMessageBundle messages,
      final boolean ephemeral,
      final boolean urgent,
      final boolean story) throws StatusException, RateLimitExceededException {

    final SpamCheckResult<GrpcResponse<SendMessageResponse>> spamCheckResult =
        spamChecker.checkForIndividualRecipientSpamGrpc(
            story ? MessageType.INDIVIDUAL_STORY : MessageType.INDIVIDUAL_SEALED_SENDER,
            Optional.empty(),
            Optional.of(destination),
            destinationServiceIdentifier);

    if (spamCheckResult.response().isPresent()) {
      return spamCheckResult.response().get().getResponseOrThrowStatus();
    }

    try {
      final int totalPayloadLength = messages.getMessagesMap().values().stream()
          .mapToInt(message -> message.getPayload().size())
          .sum();

      rateLimiters.getInboundMessageBytes().validate(destinationServiceIdentifier.uuid(), totalPayloadLength);
    } catch (final RateLimitExceededException e) {
      messageByteLimitEstimator.add(destinationServiceIdentifier.uuid().toString());
      throw e;
    }

    final Map<Byte, MessageProtos.Envelope> messagesByDeviceId = messages.getMessagesMap().entrySet()
        .stream()
        .collect(Collectors.toMap(
            entry -> DeviceIdUtil.validate(entry.getKey()),
            entry -> {
              final MessageProtos.Envelope.Builder envelopeBuilder = MessageProtos.Envelope.newBuilder()
                  .setType(MessageProtos.Envelope.Type.UNIDENTIFIED_SENDER)
                  .setClientTimestamp(messages.getTimestamp())
                  .setServerTimestamp(clock.millis())
                  .setDestinationServiceId(destinationServiceIdentifier.toServiceIdentifierString())
                  .setEphemeral(ephemeral)
                  .setUrgent(urgent)
                  .setStory(story)
                  .setContent(entry.getValue().getPayload());

              spamCheckResult.token().ifPresent(reportSpamToken ->
                  envelopeBuilder.setReportSpamToken(ByteString.copyFrom(reportSpamToken)));

              return envelopeBuilder.build();
            }
        ));

    final Map<Byte, Integer> registrationIdsByDeviceId = messages.getMessagesMap().entrySet().stream()
        .collect(Collectors.toMap(
            entry -> entry.getKey().byteValue(),
            entry -> entry.getValue().getRegistrationId()));

    return MessagesGrpcHelper.sendMessage(messageSender,
        destination,
        destinationServiceIdentifier,
        messagesByDeviceId,
        registrationIdsByDeviceId,
        Optional.empty());
  }

  @Override
  public SendMultiRecipientMessageResponse sendMultiRecipientMessage(final SendMultiRecipientMessageRequest request)
      throws StatusException {

    final SealedSenderMultiRecipientMessage multiRecipientMessage =
        parseAndValidateMultiRecipientMessage(request.getMessage().getPayload().toByteArray());

    groupSendTokenUtil.checkGroupSendToken(request.getGroupSendToken(), multiRecipientMessage.getRecipients().keySet());

    return sendMultiRecipientMessage(multiRecipientMessage,
        request.getMessage().getTimestamp(),
        request.getEphemeral(),
        request.getUrgent(),
        false);
  }

  @Override
  public SendMultiRecipientMessageResponse sendMultiRecipientStory(final SendMultiRecipientStoryRequest request)
      throws StatusException {

    final SealedSenderMultiRecipientMessage multiRecipientMessage =
        parseAndValidateMultiRecipientMessage(request.getMessage().getPayload().toByteArray());

    return sendMultiRecipientMessage(multiRecipientMessage,
        request.getMessage().getTimestamp(),
        false,
        request.getUrgent(),
        true)
        .toBuilder()
        // Don't identify unresolved recipients for stories
        .clearUnresolvedRecipients()
        .build();
  }

  private SendMultiRecipientMessageResponse sendMultiRecipientMessage(
      final SealedSenderMultiRecipientMessage multiRecipientMessage,
      final long timestamp,
      final boolean ephemeral,
      final boolean urgent,
      final boolean story) throws StatusException {

    final SpamCheckResult<GrpcResponse<SendMultiRecipientMessageResponse>> spamCheckResult =
        spamChecker.checkForMultiRecipientSpamGrpc(story
            ? MessageType.MULTI_RECIPIENT_STORY
            : MessageType.MULTI_RECIPIENT_SEALED_SENDER);

    if (spamCheckResult.response().isPresent()) {
      return spamCheckResult.response().get().getResponseOrThrowStatus();
    }

    // At this point, the caller has at least superficially provided the information needed to send a multi-recipient
    // message. Attempt to resolve the destination service identifiers to Signal accounts.
    final Map<SealedSenderMultiRecipientMessage.Recipient, Account> resolvedRecipients =
        MessageUtil.resolveRecipients(accountsManager, multiRecipientMessage);

    try {
      messageSender.sendMultiRecipientMessage(multiRecipientMessage,
          resolvedRecipients,
          timestamp,
          story,
          ephemeral,
          urgent,
          RequestAttributesUtil.getUserAgent().orElse(null));

      final SendMultiRecipientMessageResponse.Builder responseBuilder = SendMultiRecipientMessageResponse.newBuilder();

      MessageUtil.getUnresolvedRecipients(multiRecipientMessage, resolvedRecipients).stream()
          .map(ServiceIdentifierUtil::toGrpcServiceIdentifier)
          .forEach(responseBuilder::addUnresolvedRecipients);

      return responseBuilder.build();
    } catch (final MessageTooLargeException e) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Message for an individual recipient was too large")
          .withCause(e)
          .asRuntimeException();
    } catch (final MultiRecipientMismatchedDevicesException e) {
      final MultiRecipientMismatchedDevices.Builder mismatchedDevicesBuilder =
          MultiRecipientMismatchedDevices.newBuilder();

      e.getMismatchedDevicesByServiceIdentifier().forEach((serviceIdentifier, mismatchedDevices) ->
          mismatchedDevicesBuilder.addMismatchedDevices(MessagesGrpcHelper.buildMismatchedDevices(serviceIdentifier, mismatchedDevices)));

      return SendMultiRecipientMessageResponse.newBuilder()
          .setMismatchedDevices(mismatchedDevicesBuilder)
          .build();
    }
  }

  private SealedSenderMultiRecipientMessage parseAndValidateMultiRecipientMessage(
      final byte[] serializedMultiRecipientMessage) throws StatusException {

    final SealedSenderMultiRecipientMessage multiRecipientMessage;

    try {
      multiRecipientMessage = SealedSenderMultiRecipientMessage.parse(serializedMultiRecipientMessage);
    } catch (final InvalidMessageException | InvalidVersionException e) {
      throw Status.INVALID_ARGUMENT.withCause(e).asException();
    }

    // Check that the request is well-formed and doesn't contain repeated entries for the same device for the same
    // recipient
    if (MessageUtil.hasDuplicateDevices(multiRecipientMessage)) {
      throw Status.INVALID_ARGUMENT.withDescription("Multi-recipient message contains duplicate recipient").asException();
    }

    return multiRecipientMessage;
  }
}
