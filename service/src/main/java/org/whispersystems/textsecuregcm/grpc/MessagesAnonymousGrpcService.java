/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.signal.chat.messages.MismatchedDevices;
import org.signal.chat.messages.MultiRecipientMismatchedDevices;
import org.signal.chat.messages.SendMessageResponse;
import org.signal.chat.messages.SendMultiRecipientMessageRequest;
import org.signal.chat.messages.SendMultiRecipientMessageResponse;
import org.signal.chat.messages.SendSealedSenderMessageRequest;
import org.signal.chat.messages.SimpleMessagesAnonymousGrpc;
import org.signal.libsignal.protocol.InvalidMessageException;
import org.signal.libsignal.protocol.InvalidVersionException;
import org.signal.libsignal.protocol.SealedSenderMultiRecipientMessage;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.MultiRecipientMismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.CardinalityEstimator;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.MessageTooLargeException;
import org.whispersystems.textsecuregcm.spam.GrpcResponse;
import org.whispersystems.textsecuregcm.spam.MessageType;
import org.whispersystems.textsecuregcm.spam.SpamCheckResult;
import org.whispersystems.textsecuregcm.spam.SpamChecker;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

public class MessagesAnonymousGrpcService extends SimpleMessagesAnonymousGrpc.MessagesAnonymousImplBase {

  private final AccountsManager accountsManager;
  private final RateLimiters rateLimiters;
  private final MessageSender messageSender;
  private final GroupSendTokenUtil groupSendTokenUtil;
  private final CardinalityEstimator messageByteLimitEstimator;
  private final SpamChecker spamChecker;
  private final Clock clock;

  private static final SendMessageResponse SEND_MESSAGE_SUCCESS_RESPONSE = SendMessageResponse.newBuilder().build();

  private static final int MAX_FETCH_ACCOUNT_CONCURRENCY = 8;

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

    final SpamCheckResult<GrpcResponse<SendMessageResponse>> spamCheckResult =
        spamChecker.checkForIndividualRecipientSpamGrpc(MessageType.INDIVIDUAL_SEALED_SENDER,
            Optional.empty(),
            Optional.of(destination),
            destinationServiceIdentifier);

    if (spamCheckResult.response().isPresent()) {
      return spamCheckResult.response().get().getResponseOrThrowStatus();
    }

    try {
      final int totalPayloadLength = request.getMessages().getMessagesMap().values().stream()
          .mapToInt(message -> message.getPayload().size())
          .sum();

      rateLimiters.getInboundMessageBytes().validate(destinationServiceIdentifier.uuid(), totalPayloadLength);
    } catch (final RateLimitExceededException e) {
      messageByteLimitEstimator.add(destinationServiceIdentifier.uuid().toString());
      throw e;
    }

    final Map<Byte, MessageProtos.Envelope> messagesByDeviceId = request.getMessages().getMessagesMap().entrySet()
        .stream()
        .collect(Collectors.toMap(
            entry -> DeviceIdUtil.validate(entry.getKey()),
            entry -> {
              final MessageProtos.Envelope.Builder envelopeBuilder = MessageProtos.Envelope.newBuilder()
                  .setType(MessageProtos.Envelope.Type.UNIDENTIFIED_SENDER)
                  .setClientTimestamp(request.getMessages().getTimestamp())
                  .setServerTimestamp(clock.millis())
                  .setDestinationServiceId(destinationServiceIdentifier.toServiceIdentifierString())
                  .setEphemeral(request.getEphemeral())
                  .setUrgent(request.getUrgent())
                  .setContent(entry.getValue().getPayload());

              spamCheckResult.token()
                  .ifPresent(token -> envelopeBuilder.setReportSpamToken(ByteString.copyFrom(token)));

              return envelopeBuilder.build();
            }
        ));

    final Map<Byte, Integer> registrationIdsByDeviceId = request.getMessages().getMessagesMap().entrySet().stream()
        .collect(Collectors.toMap(
            entry -> entry.getKey().byteValue(),
            entry -> entry.getValue().getRegistrationId()));

    try {
      messageSender.sendMessages(destination,
          destinationServiceIdentifier,
          messagesByDeviceId,
          registrationIdsByDeviceId,
          RequestAttributesUtil.getRawUserAgent().orElse(null));

      return SEND_MESSAGE_SUCCESS_RESPONSE;
    } catch (final MismatchedDevicesException e) {
      return SendMessageResponse.newBuilder()
          .setMismatchedDevices(buildMismatchedDevices(destinationServiceIdentifier, e.getMismatchedDevices()))
          .build();
    } catch (final MessageTooLargeException e) {
      throw Status.INVALID_ARGUMENT.withDescription("Message too large").withCause(e).asException();
    }
  }

  @Override
  public SendMultiRecipientMessageResponse sendMultiRecipientMessage(final SendMultiRecipientMessageRequest request)
      throws StatusException {

    final SealedSenderMultiRecipientMessage multiRecipientMessage;

    try {
      multiRecipientMessage = SealedSenderMultiRecipientMessage.parse(request.getMessage().getPayload().toByteArray());
    } catch (final InvalidMessageException | InvalidVersionException e) {
      throw Status.INVALID_ARGUMENT.withCause(e).asException();
    }

    // Check that the request is well-formed and doesn't contain repeated entries for the same device for the same
    // recipient
    {
      final boolean[] usedDeviceIds = new boolean[Device.MAXIMUM_DEVICE_ID];

      for (final SealedSenderMultiRecipientMessage.Recipient recipient : multiRecipientMessage.getRecipients().values()) {
        Arrays.fill(usedDeviceIds, false);

        for (final byte deviceId : recipient.getDevices()) {
          if (usedDeviceIds[deviceId]) {
            throw Status.INVALID_ARGUMENT.withDescription("Request contains repeated device entries").asException();
          }

          usedDeviceIds[deviceId] = true;
        }
      }
    }

    groupSendTokenUtil.checkGroupSendToken(request.getGroupSendToken(), multiRecipientMessage.getRecipients().keySet());

    final SpamCheckResult<GrpcResponse<SendMultiRecipientMessageResponse>> spamCheckResult =
        spamChecker.checkForMultiRecipientSpamGrpc(MessageType.MULTI_RECIPIENT_SEALED_SENDER);

    if (spamCheckResult.response().isPresent()) {
      return spamCheckResult.response().get().getResponseOrThrowStatus();
    }

    // At this point, the caller has at least superficially provided the information needed to send a multi-recipient
    // message. Attempt to resolve the destination service identifiers to Signal accounts.
    final Map<SealedSenderMultiRecipientMessage.Recipient, Account> resolvedRecipients =
        Flux.fromIterable(multiRecipientMessage.getRecipients().entrySet())
            .flatMap(serviceIdAndRecipient -> {
              final ServiceIdentifier serviceIdentifier =
                  ServiceIdentifier.fromLibsignal(serviceIdAndRecipient.getKey());

              return Mono.fromFuture(() -> accountsManager.getByServiceIdentifierAsync(serviceIdentifier))
                  .flatMap(Mono::justOrEmpty)
                  .map(account -> Tuples.of(serviceIdAndRecipient.getValue(), account));
            }, MAX_FETCH_ACCOUNT_CONCURRENCY)
            .collectMap(Tuple2::getT1, Tuple2::getT2)
            .blockOptional()
            .orElse(Collections.emptyMap());

    try {
      messageSender.sendMultiRecipientMessage(multiRecipientMessage,
          resolvedRecipients,
          request.getMessage().getTimestamp(),
          false,
          request.getEphemeral(),
          request.getUrgent(),
          RequestAttributesUtil.getRawUserAgent().orElse(null));

      final SendMultiRecipientMessageResponse.Builder responseBuilder = SendMultiRecipientMessageResponse.newBuilder();

      multiRecipientMessage.getRecipients().entrySet()
          .stream()
          .filter(entry -> !resolvedRecipients.containsKey(entry.getValue()))
          .map(entry -> ServiceIdentifier.fromLibsignal(entry.getKey()))
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
          mismatchedDevicesBuilder.addMismatchedDevices(buildMismatchedDevices(serviceIdentifier, mismatchedDevices)));

      return SendMultiRecipientMessageResponse.newBuilder()
          .setMismatchedDevices(mismatchedDevicesBuilder)
          .build();
    }
  }

  private MismatchedDevices buildMismatchedDevices(final ServiceIdentifier serviceIdentifier,
      org.whispersystems.textsecuregcm.controllers.MismatchedDevices mismatchedDevices) {

    final MismatchedDevices.Builder mismatchedDevicesBuilder = MismatchedDevices.newBuilder()
        .setServiceIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier));

    mismatchedDevices.missingDeviceIds().forEach(mismatchedDevicesBuilder::addMissingDevices);
    mismatchedDevices.extraDeviceIds().forEach(mismatchedDevicesBuilder::addExtraDevices);
    mismatchedDevices.staleDeviceIds().forEach(mismatchedDevicesBuilder::addStaleDevices);

    return mismatchedDevicesBuilder.build();
  }
}
