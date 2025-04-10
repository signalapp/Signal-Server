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
import org.signal.chat.messages.AuthenticatedSenderMessageType;
import org.signal.chat.messages.IndividualRecipientMessageBundle;
import org.signal.chat.messages.SendAuthenticatedSenderMessageRequest;
import org.signal.chat.messages.SendMessageResponse;
import org.signal.chat.messages.SendSyncMessageRequest;
import org.signal.chat.messages.SimpleMessagesGrpc;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.CardinalityEstimator;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.spam.GrpcResponse;
import org.whispersystems.textsecuregcm.spam.MessageType;
import org.whispersystems.textsecuregcm.spam.SpamCheckResult;
import org.whispersystems.textsecuregcm.spam.SpamChecker;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;

public class MessagesGrpcService extends SimpleMessagesGrpc.MessagesImplBase {

  private final AccountsManager accountsManager;
  private final RateLimiters rateLimiters;
  private final MessageSender messageSender;
  private final CardinalityEstimator messageByteLimitEstimator;
  private final SpamChecker spamChecker;
  private final Clock clock;

  public MessagesGrpcService(final AccountsManager accountsManager,
      final RateLimiters rateLimiters,
      final MessageSender messageSender,
      final CardinalityEstimator messageByteLimitEstimator,
      final SpamChecker spamChecker,
      final Clock clock) {

    this.accountsManager = accountsManager;
    this.rateLimiters = rateLimiters;
    this.messageSender = messageSender;
    this.messageByteLimitEstimator = messageByteLimitEstimator;
    this.spamChecker = spamChecker;
    this.clock = clock;
  }

  @Override
  public SendMessageResponse sendMessage(final SendAuthenticatedSenderMessageRequest request)
      throws StatusException, RateLimitExceededException {

    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
    final AciServiceIdentifier senderServiceIdentifier = new AciServiceIdentifier(authenticatedDevice.accountIdentifier());
    final Account sender =
        accountsManager.getByServiceIdentifier(senderServiceIdentifier).orElseThrow(Status.UNAUTHENTICATED::asException);

    final ServiceIdentifier destinationServiceIdentifier =
        ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getDestination());

    if (sender.isIdentifiedBy(destinationServiceIdentifier)) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Use `sendSyncMessage` to send messages to own account")
          .asException();
    }

    final Account destination = accountsManager.getByServiceIdentifier(destinationServiceIdentifier)
        .orElseThrow(Status.NOT_FOUND::asException);

    rateLimiters.getMessagesLimiter().validate(authenticatedDevice.accountIdentifier(), destination.getUuid());

    return sendMessage(destination,
        destinationServiceIdentifier,
        authenticatedDevice,
        request.getType(),
        MessageType.INDIVIDUAL_IDENTIFIED_SENDER,
        request.getMessages(),
        request.getEphemeral(),
        request.getUrgent());
  }

  @Override
  public SendMessageResponse sendSyncMessage(final SendSyncMessageRequest request)
      throws StatusException, RateLimitExceededException {

    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();
    final AciServiceIdentifier senderServiceIdentifier = new AciServiceIdentifier(authenticatedDevice.accountIdentifier());
    final Account sender =
        accountsManager.getByServiceIdentifier(senderServiceIdentifier).orElseThrow(Status.UNAUTHENTICATED::asException);

    return sendMessage(sender,
        senderServiceIdentifier,
        authenticatedDevice,
        request.getType(),
        MessageType.SYNC,
        request.getMessages(),
        false,
        request.getUrgent());
  }

  private SendMessageResponse sendMessage(final Account destination,
      final ServiceIdentifier destinationServiceIdentifier,
      final AuthenticatedDevice sender,
      final AuthenticatedSenderMessageType envelopeType,
      final MessageType messageType,
      final IndividualRecipientMessageBundle messages,
      final boolean ephemeral,
      final boolean urgent) throws StatusException, RateLimitExceededException {

    try {
      final int totalPayloadLength = messages.getMessagesMap().values().stream()
          .mapToInt(message -> message.getPayload().size())
          .sum();

      rateLimiters.getInboundMessageBytes().validate(destinationServiceIdentifier.uuid(), totalPayloadLength);
    } catch (final RateLimitExceededException e) {
      messageByteLimitEstimator.add(destinationServiceIdentifier.uuid().toString());
      throw e;
    }

    final SpamCheckResult<GrpcResponse<SendMessageResponse>> spamCheckResult =
        spamChecker.checkForIndividualRecipientSpamGrpc(messageType,
            Optional.of(sender),
            Optional.of(destination),
            destinationServiceIdentifier);

    if (spamCheckResult.response().isPresent()) {
      return spamCheckResult.response().get().getResponseOrThrowStatus();
    }

    final Map<Byte, MessageProtos.Envelope> messagesByDeviceId = messages.getMessagesMap().entrySet()
        .stream()
        .collect(Collectors.toMap(
            entry -> DeviceIdUtil.validate(entry.getKey()),
            entry -> {
              final MessageProtos.Envelope.Builder envelopeBuilder = MessageProtos.Envelope.newBuilder()
                  .setType(getEnvelopeType(envelopeType))
                  .setClientTimestamp(messages.getTimestamp())
                  .setServerTimestamp(clock.millis())
                  .setDestinationServiceId(destinationServiceIdentifier.toServiceIdentifierString())
                  .setSourceServiceId(new AciServiceIdentifier(sender.accountIdentifier()).toServiceIdentifierString())
                  .setSourceDevice(sender.deviceId())
                  .setEphemeral(ephemeral)
                  .setUrgent(urgent)
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
        messageType == MessageType.SYNC ? Optional.of(sender.deviceId()) : Optional.empty());
  }

  private static MessageProtos.Envelope.Type getEnvelopeType(final AuthenticatedSenderMessageType type) {
    return switch (type) {
      case DOUBLE_RATCHET -> MessageProtos.Envelope.Type.CIPHERTEXT;
      case PREKEY_MESSAGE -> MessageProtos.Envelope.Type.PREKEY_BUNDLE;
      case PLAINTEXT_CONTENT -> MessageProtos.Envelope.Type.PLAINTEXT_CONTENT;
      case UNSPECIFIED, UNRECOGNIZED ->
          throw Status.INVALID_ARGUMENT.withDescription("Unrecognized envelope type").asRuntimeException();
    };
  }
}
