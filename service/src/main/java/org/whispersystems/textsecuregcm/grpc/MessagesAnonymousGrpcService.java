/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import java.time.Clock;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import com.google.protobuf.Empty;
import org.signal.chat.errors.FailedUnidentifiedAuthorization;
import org.signal.chat.errors.NotFound;
import org.signal.chat.messages.IndividualRecipientMessageBundle;
import org.signal.chat.messages.MultiRecipientMismatchedDevices;
import org.signal.chat.messages.MultiRecipientSuccess;
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
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
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
import org.whispersystems.textsecuregcm.spam.GrpcChallengeResponse;
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

  private static final SendMessageResponse SEND_MESSAGE_SUCCESS_RESPONSE = SendMessageResponse
      .newBuilder()
      .setSuccess(Empty.getDefaultInstance())
      .build();

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
    this.groupSendTokenUtil = groupSendTokenUtil;
    this.messageByteLimitEstimator = messageByteLimitEstimator;
    this.spamChecker = spamChecker;
    this.clock = clock;
  }

  @Override
  public SendMessageResponse sendSingleRecipientMessage(final SendSealedSenderMessageRequest request)
      throws RateLimitExceededException {

    final ServiceIdentifier destinationServiceIdentifier =
        ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getDestination());

    final Optional<Account> maybeDestination = accountsManager.getByServiceIdentifier(destinationServiceIdentifier);

    final boolean authorized = switch (request.getAuthorizationCase()) {
      case UNIDENTIFIED_ACCESS_KEY -> {
        if (destinationServiceIdentifier.identityType() == IdentityType.PNI) {
          throw GrpcExceptions.fieldViolation("authorization",
              "message for PNI cannot be authenticated with an unidentified access token");
        }
        final byte[] uak = request.getUnidentifiedAccessKey().toByteArray();
        yield maybeDestination
            .map(account -> UnidentifiedAccessUtil.checkUnidentifiedAccess(account, uak))
            // If the destination is not found, return an authorization error instead of not-found. Otherwise,
            // this would provide an unauthenticated existence check.
            .orElse(false);
      }
      case GROUP_SEND_TOKEN ->
          groupSendTokenUtil.checkGroupSendToken(request.getGroupSendToken(), destinationServiceIdentifier);
      case AUTHORIZATION_NOT_SET ->
          throw GrpcExceptions.fieldViolation("authorization", "expected authorization token not provided");
    };

    if (!authorized) {
      return SendMessageResponse.newBuilder()
          .setFailedUnidentifiedAuthorization(FailedUnidentifiedAuthorization.getDefaultInstance())
          .build();
    }

    if (maybeDestination.isEmpty()) {
      return SendMessageResponse.newBuilder().setDestinationNotFound(NotFound.getDefaultInstance()).build();
    }
    
    return sendIndividualMessage(maybeDestination.get(),
        destinationServiceIdentifier,
        request.getMessages(),
        request.getEphemeral(),
        request.getUrgent(),
        false);
  }

  @Override
  public SendMessageResponse sendStory(final SendStoryMessageRequest request)
      throws RateLimitExceededException {

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
      final boolean story) throws RateLimitExceededException {

    final SpamCheckResult<GrpcChallengeResponse> spamCheckResult =
        spamChecker.checkForIndividualRecipientSpamGrpc(
            story ? MessageType.INDIVIDUAL_STORY : MessageType.INDIVIDUAL_SEALED_SENDER,
            Optional.empty(),
            Optional.of(destination),
            destinationServiceIdentifier);

    spamCheckResult.response().ifPresent(grpcResponse ->
      grpcResponse.throwStatusOr(_ -> GrpcExceptions.rateLimitExceeded(null)));

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
                  .setContent(entry.getValue().getPayload());

              if (story) {
                // Avoid sending this field if it's false.
                envelopeBuilder.setStory(true);
              }

              spamCheckResult.token().ifPresent(reportSpamToken ->
                  envelopeBuilder.setReportSpamToken(ByteString.copyFrom(reportSpamToken)));

              return envelopeBuilder.build();
            }
        ));

    final Map<Byte, Integer> registrationIdsByDeviceId = messages.getMessagesMap().entrySet().stream()
        .collect(Collectors.toMap(
            entry -> entry.getKey().byteValue(),
            entry -> entry.getValue().getRegistrationId()));

    try {
      messageSender.sendMessages(destination,
          destinationServiceIdentifier,
          messagesByDeviceId,
          registrationIdsByDeviceId,
          Optional.empty(),
          RequestAttributesUtil.getUserAgent().orElse(null));

      return SEND_MESSAGE_SUCCESS_RESPONSE;
    } catch (final MismatchedDevicesException e) {
      return SendMessageResponse.newBuilder()
          .setMismatchedDevices(MessagesGrpcHelper.buildMismatchedDevices(destinationServiceIdentifier, e.getMismatchedDevices()))
          .build();
    } catch (final MessageTooLargeException e) {
      throw GrpcExceptions.invalidArguments("message too large");
    }
  }

  @Override
  public SendMultiRecipientMessageResponse sendMultiRecipientMessage(final SendMultiRecipientMessageRequest request) {

    final SealedSenderMultiRecipientMessage multiRecipientMessage =
        parseAndValidateMultiRecipientMessage(request.getMessage().getPayload().toByteArray());

    if (!groupSendTokenUtil.checkGroupSendToken(request.getGroupSendToken(), multiRecipientMessage.getRecipients().keySet())) {
      return SendMultiRecipientMessageResponse.newBuilder()
          .setFailedUnidentifiedAuthorization(FailedUnidentifiedAuthorization.getDefaultInstance())
          .build();
    }

    return sendMultiRecipientMessage(multiRecipientMessage,
        request.getMessage().getTimestamp(),
        request.getEphemeral(),
        request.getUrgent(),
        false);
  }

  @Override
  public SendMultiRecipientMessageResponse sendMultiRecipientStory(final SendMultiRecipientStoryRequest request) {

    final SealedSenderMultiRecipientMessage multiRecipientMessage =
        parseAndValidateMultiRecipientMessage(request.getMessage().getPayload().toByteArray());

    final SendMultiRecipientMessageResponse sendMultiRecipientMessageResponse = sendMultiRecipientMessage(
        multiRecipientMessage,
        request.getMessage().getTimestamp(),
        false,
        request.getUrgent(),
        true);
    if (sendMultiRecipientMessageResponse.hasSuccess()) {
      // Clear the unresolved recipients for stories
      return sendMultiRecipientMessageResponse.toBuilder()
          .setSuccess(MultiRecipientSuccess.getDefaultInstance())
          .build();
    } else {
      return sendMultiRecipientMessageResponse;
    }

  }

  private SendMultiRecipientMessageResponse sendMultiRecipientMessage(
      final SealedSenderMultiRecipientMessage multiRecipientMessage,
      final long timestamp,
      final boolean ephemeral,
      final boolean urgent,
      final boolean story) {

    final SpamCheckResult<GrpcChallengeResponse> spamCheckResult =
        spamChecker.checkForMultiRecipientSpamGrpc(story
            ? MessageType.MULTI_RECIPIENT_STORY
            : MessageType.MULTI_RECIPIENT_SEALED_SENDER);

    spamCheckResult.response().ifPresent(response ->
        response.throwStatusOr(_ -> GrpcExceptions.rateLimitExceeded(null)));

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
          RequestAttributesUtil.getUserAgent().orElse(null))
          .join();

      final MultiRecipientSuccess.Builder responseBuilder = MultiRecipientSuccess.newBuilder();

      MessageUtil.getUnresolvedRecipients(multiRecipientMessage, resolvedRecipients).stream()
          .map(ServiceIdentifierUtil::toGrpcServiceIdentifier)
          .forEach(responseBuilder::addUnresolvedRecipients);

      return SendMultiRecipientMessageResponse.newBuilder().setSuccess(responseBuilder).build();
    } catch (final MessageTooLargeException e) {
      throw GrpcExceptions.invalidArguments("message for an individual recipient was too large");
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
      final byte[] serializedMultiRecipientMessage) {

    final SealedSenderMultiRecipientMessage multiRecipientMessage;

    try {
      multiRecipientMessage = SealedSenderMultiRecipientMessage.parse(serializedMultiRecipientMessage);
    } catch (final InvalidMessageException _) {
      throw GrpcExceptions.fieldViolation("payload", "invalid multi-recipient message");
    } catch (final InvalidVersionException e) {
      throw GrpcExceptions.fieldViolation("payload", "unrecognized sealed sender major version");
    }

    if (multiRecipientMessage.getRecipients().isEmpty()) {
      throw GrpcExceptions.fieldViolation("payload", "recipient list is empty");
    }

    // Check that the request is well-formed and doesn't contain repeated entries for the same device for the same
    // recipient
    if (MessageUtil.hasDuplicateDevices(multiRecipientMessage)) {
      throw GrpcExceptions.fieldViolation("payload", "multi-recipient message contains duplicate recipient");
    }

    return multiRecipientMessage;
  }

}
