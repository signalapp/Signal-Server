/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.mockito.Mock;
import org.signal.chat.messages.AuthenticatedSenderMessageType;
import org.signal.chat.messages.ChallengeRequired;
import org.signal.chat.messages.IndividualRecipientMessageBundle;
import org.signal.chat.messages.MessagesGrpc;
import org.signal.chat.messages.MismatchedDevices;
import org.signal.chat.messages.SendAuthenticatedSenderMessageRequest;
import org.signal.chat.messages.SendMessageResponse;
import org.signal.chat.messages.SendSyncMessageRequest;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.CardinalityEstimator;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
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
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class MessagesGrpcServiceTest extends SimpleBaseGrpcTest<MessagesGrpcService, MessagesGrpc.MessagesBlockingStub> {

  @Mock
  private AccountsManager accountsManager;

  @Mock
  private RateLimiters rateLimiters;

  @Mock
  private MessageSender messageSender;

  @Mock
  private CardinalityEstimator messageByteLimitEstimator;

  @Mock
  private SpamChecker spamChecker;

  @Mock
  private RateLimiter rateLimiter;

  @Mock
  private Account authenticatedAccount;

  @Mock
  private Device authenticatedDevice;

  @Mock
  private Device linkedDevice;

  @Mock
  private Device secondLinkedDevice;

  private static final int AUTHENTICATED_REGISTRATION_ID = 7;

  private static final byte LINKED_DEVICE_ID = AUTHENTICATED_DEVICE_ID + 1;
  private static final int LINKED_DEVICE_REGISTRATION_ID = 13;

  private static final byte SECOND_LINKED_DEVICE_ID = LINKED_DEVICE_ID + 1;
  private static final int SECOND_LINKED_DEVICE_REGISTRATION_ID = 19;

  private static final TestClock CLOCK = TestClock.pinned(Instant.now());

  @Override
  protected MessagesGrpcService createServiceBeforeEachTest() {
    return new MessagesGrpcService(accountsManager,
        rateLimiters,
        messageSender,
        messageByteLimitEstimator,
        spamChecker,
        CLOCK);
  }

  @BeforeEach
  void setUp() {
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.empty());

    when(rateLimiters.getInboundMessageBytes()).thenReturn(rateLimiter);
    when(rateLimiters.getMessagesLimiter()).thenReturn(rateLimiter);

    when(spamChecker.checkForIndividualRecipientSpamGrpc(any(), any(), any(), any()))
        .thenReturn(new SpamCheckResult<>(Optional.empty(), Optional.empty()));

    when(authenticatedDevice.getId()).thenReturn(AUTHENTICATED_DEVICE_ID);
    when(authenticatedDevice.getRegistrationId(IdentityType.ACI)).thenReturn(AUTHENTICATED_REGISTRATION_ID);

    when(linkedDevice.getId()).thenReturn(LINKED_DEVICE_ID);
    when(linkedDevice.getRegistrationId(IdentityType.ACI)).thenReturn(LINKED_DEVICE_REGISTRATION_ID);

    when(secondLinkedDevice.getId()).thenReturn(SECOND_LINKED_DEVICE_ID);
    when(secondLinkedDevice.getRegistrationId(IdentityType.ACI)).thenReturn(SECOND_LINKED_DEVICE_REGISTRATION_ID);

    when(authenticatedAccount.getUuid()).thenReturn(AUTHENTICATED_ACI);
    when(authenticatedAccount.getIdentifier(IdentityType.ACI)).thenReturn(AUTHENTICATED_ACI);
    when(authenticatedAccount.getDevice(anyByte())).thenReturn(Optional.empty());
    when(authenticatedAccount.getDevice(AUTHENTICATED_DEVICE_ID)).thenReturn(Optional.of(authenticatedDevice));
    when(authenticatedAccount.getDevice(LINKED_DEVICE_ID)).thenReturn(Optional.of(linkedDevice));
    when(authenticatedAccount.getDevice(SECOND_LINKED_DEVICE_ID)).thenReturn(Optional.of(secondLinkedDevice));
    when(authenticatedAccount.getDevices()).thenReturn(List.of(authenticatedDevice, linkedDevice, secondLinkedDevice));

    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(AUTHENTICATED_ACI)))
        .thenReturn(Optional.of(authenticatedAccount));
  }

  @Nested
  class SingleRecipient {

    @CartesianTest
    void sendMessage(@CartesianTest.Enum(mode = CartesianTest.Enum.Mode.EXCLUDE, names = {"UNSPECIFIED", "UNRECOGNIZED"}) final AuthenticatedSenderMessageType messageType,
        @CartesianTest.Values(booleans = {true, false}) final boolean ephemeral,
        @CartesianTest.Values(booleans = {true, false}) final boolean urgent,
        @CartesianTest.Values(booleans = {true, false}) final boolean includeReportSpamToken)
        throws MessageTooLargeException, MismatchedDevicesException {

      final byte deviceId = Device.PRIMARY_ID;
      final int registrationId = 7;

      final Device destinationDevice = DevicesHelper.createDevice(deviceId, CLOCK.millis(), registrationId);

      final Account destinationAccount = mock(Account.class);
      when(destinationAccount.getDevices()).thenReturn(List.of(destinationDevice));
      when(destinationAccount.getDevice(deviceId)).thenReturn(Optional.of(destinationDevice));

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
      when(accountsManager.getByServiceIdentifier(serviceIdentifier)).thenReturn(Optional.of(destinationAccount));

      final byte[] reportSpamToken = TestRandomUtil.nextBytes(64);

      if (includeReportSpamToken) {
        when(spamChecker.checkForIndividualRecipientSpamGrpc(any(), any(), any(), any()))
            .thenReturn(new SpamCheckResult<>(Optional.empty(), Optional.of(reportSpamToken)));
      }

      final byte[] payload = TestRandomUtil.nextBytes(128);

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages =
          Map.of(deviceId, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(registrationId)
              .setPayload(ByteString.copyFrom(payload))
              .build());

      final SendMessageResponse response = authenticatedServiceStub().sendMessage(
          generateRequest(serviceIdentifier, messageType, ephemeral, urgent, messages));

      assertEquals(SendMessageResponse.newBuilder().build(), response);

      final MessageProtos.Envelope.Type expectedEnvelopeType = switch (messageType) {
        case DOUBLE_RATCHET -> MessageProtos.Envelope.Type.CIPHERTEXT;
        case PREKEY_MESSAGE -> MessageProtos.Envelope.Type.PREKEY_BUNDLE;
        case PLAINTEXT_CONTENT -> MessageProtos.Envelope.Type.PLAINTEXT_CONTENT;
        case UNSPECIFIED, UNRECOGNIZED -> throw new IllegalArgumentException("Unexpected message type: " + messageType);
      };

      final MessageProtos.Envelope.Builder expectedEnvelopeBuilder = MessageProtos.Envelope.newBuilder()
          .setType(expectedEnvelopeType)
          .setSourceServiceId(new AciServiceIdentifier(AUTHENTICATED_ACI).toServiceIdentifierString())
          .setSourceDevice(AUTHENTICATED_DEVICE_ID)
          .setDestinationServiceId(serviceIdentifier.toServiceIdentifierString())
          .setClientTimestamp(CLOCK.millis())
          .setServerTimestamp(CLOCK.millis())
          .setEphemeral(ephemeral)
          .setUrgent(urgent)
          .setContent(ByteString.copyFrom(payload));

      if (includeReportSpamToken) {
        expectedEnvelopeBuilder.setReportSpamToken(ByteString.copyFrom(reportSpamToken));
      }

      verify(spamChecker).checkForIndividualRecipientSpamGrpc(MessageType.INDIVIDUAL_IDENTIFIED_SENDER,
          Optional.of(new AuthenticatedDevice(AUTHENTICATED_ACI, AUTHENTICATED_DEVICE_ID)),
          Optional.of(destinationAccount),
          serviceIdentifier);

      verify(messageSender).sendMessages(destinationAccount,
          serviceIdentifier,
          Map.of(deviceId, expectedEnvelopeBuilder.build()),
          Map.of(deviceId, registrationId),
          Optional.empty(),
          null);
    }

    @Test
    void mismatchedDevices() throws MessageTooLargeException, MismatchedDevicesException {
      final byte missingDeviceId = Device.PRIMARY_ID;
      final byte extraDeviceId = missingDeviceId + 1;
      final byte staleDeviceId = extraDeviceId + 1;

      final Account destinationAccount = mock(Account.class);

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
      when(accountsManager.getByServiceIdentifier(serviceIdentifier)).thenReturn(Optional.of(destinationAccount));

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages = Map.of(
          staleDeviceId, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(Device.PRIMARY_ID)
              .setPayload(ByteString.copyFrom(TestRandomUtil.nextBytes(128)))
              .build());

      doThrow(new MismatchedDevicesException(new org.whispersystems.textsecuregcm.controllers.MismatchedDevices(
          Set.of(missingDeviceId), Set.of(extraDeviceId), Set.of(staleDeviceId))))
          .when(messageSender).sendMessages(any(), any(), any(), any(), any(), any());

      final SendMessageResponse response = authenticatedServiceStub().sendMessage(
          generateRequest(serviceIdentifier, AuthenticatedSenderMessageType.DOUBLE_RATCHET, false, true, messages));

      final SendMessageResponse expectedResponse = SendMessageResponse.newBuilder()
          .setMismatchedDevices(MismatchedDevices.newBuilder()
              .setServiceIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier))
              .addMissingDevices(missingDeviceId)
              .addStaleDevices(staleDeviceId)
              .addExtraDevices(extraDeviceId)
              .build())
          .build();

      assertEquals(expectedResponse, response);
    }

    @Test
    void destinationNotFound() throws MessageTooLargeException, MismatchedDevicesException {
      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages =
          Map.of(Device.PRIMARY_ID, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(1234)
              .setPayload(ByteString.copyFrom(TestRandomUtil.nextBytes(128)))
              .build());

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusException(Status.NOT_FOUND,
          () -> authenticatedServiceStub().sendMessage(
              generateRequest(serviceIdentifier, AuthenticatedSenderMessageType.DOUBLE_RATCHET, false, true, messages)));

      verify(messageSender, never()).sendMessages(any(), any(), any(), any(), any(), any());
    }

    @Test
    void rateLimited() throws RateLimitExceededException, MessageTooLargeException, MismatchedDevicesException {
      final byte deviceId = Device.PRIMARY_ID;
      final int registrationId = 7;

      final Device destinationDevice = DevicesHelper.createDevice(deviceId, CLOCK.millis(), registrationId);

      final Account destinationAccount = mock(Account.class);
      when(destinationAccount.getDevices()).thenReturn(List.of(destinationDevice));
      when(destinationAccount.getDevice(deviceId)).thenReturn(Optional.of(destinationDevice));

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
      when(accountsManager.getByServiceIdentifier(serviceIdentifier)).thenReturn(Optional.of(destinationAccount));

      final Duration retryDuration = Duration.ofHours(7);

      doThrow(new RateLimitExceededException(retryDuration))
          .when(rateLimiter).validate(eq(serviceIdentifier.uuid()), anyInt());

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages =
          Map.of(deviceId, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(registrationId)
              .setPayload(ByteString.copyFrom(TestRandomUtil.nextBytes(128)))
              .build());

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertRateLimitExceeded(retryDuration,
          () -> authenticatedServiceStub().sendMessage(
              generateRequest(serviceIdentifier, AuthenticatedSenderMessageType.DOUBLE_RATCHET, false, true, messages)));

      verify(messageSender, never()).sendMessages(any(), any(), any(), any(), any(), any());
      verify(messageByteLimitEstimator).add(serviceIdentifier.uuid().toString());
    }

    @Test
    void oversizedMessage() throws MessageTooLargeException, MismatchedDevicesException {
      final byte missingDeviceId = Device.PRIMARY_ID;
      final byte extraDeviceId = missingDeviceId + 1;
      final byte staleDeviceId = extraDeviceId + 1;

      final Account destinationAccount = mock(Account.class);

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
      when(accountsManager.getByServiceIdentifier(serviceIdentifier)).thenReturn(Optional.of(destinationAccount));

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages = Map.of(
          staleDeviceId, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(Device.PRIMARY_ID)
              .setPayload(ByteString.copyFrom(TestRandomUtil.nextBytes(128)))
              .build());

      doThrow(new MessageTooLargeException())
          .when(messageSender).sendMessages(any(), any(), any(), any(), any(), any());

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
          () -> authenticatedServiceStub().sendMessage(
              generateRequest(serviceIdentifier, AuthenticatedSenderMessageType.DOUBLE_RATCHET, false, true, messages)));
    }

    @Test
    void spamWithStatus() throws MessageTooLargeException, MismatchedDevicesException {
      final byte deviceId = Device.PRIMARY_ID;
      final int registrationId = 7;

      final Device destinationDevice = DevicesHelper.createDevice(deviceId, CLOCK.millis(), registrationId);

      final Account destinationAccount = mock(Account.class);
      when(destinationAccount.getDevices()).thenReturn(List.of(destinationDevice));
      when(destinationAccount.getDevice(deviceId)).thenReturn(Optional.of(destinationDevice));

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
      when(accountsManager.getByServiceIdentifier(serviceIdentifier)).thenReturn(Optional.of(destinationAccount));

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages =
          Map.of(deviceId, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(registrationId)
              .setPayload(ByteString.copyFrom(TestRandomUtil.nextBytes(128)))
              .build());

      when(spamChecker.checkForIndividualRecipientSpamGrpc(any(), any(), any(), any()))
          .thenReturn(new SpamCheckResult<>(Optional.of(GrpcResponse.withStatus(Status.RESOURCE_EXHAUSTED)), Optional.empty()));

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusException(Status.RESOURCE_EXHAUSTED,
          () -> authenticatedServiceStub().sendMessage(
              generateRequest(serviceIdentifier, AuthenticatedSenderMessageType.DOUBLE_RATCHET, false, true, messages)));

      verify(spamChecker).checkForIndividualRecipientSpamGrpc(MessageType.INDIVIDUAL_IDENTIFIED_SENDER,
          Optional.of(new AuthenticatedDevice(AUTHENTICATED_ACI, AUTHENTICATED_DEVICE_ID)),
          Optional.of(destinationAccount),
          serviceIdentifier);

      verify(messageSender, never()).sendMessages(any(), any(), any(), any(), any(), any());
    }

    @Test
    void spamWithResponse() throws MessageTooLargeException, MismatchedDevicesException {
      final byte deviceId = Device.PRIMARY_ID;
      final int registrationId = 7;

      final Device destinationDevice = DevicesHelper.createDevice(deviceId, CLOCK.millis(), registrationId);

      final Account destinationAccount = mock(Account.class);
      when(destinationAccount.getDevices()).thenReturn(List.of(destinationDevice));
      when(destinationAccount.getDevice(deviceId)).thenReturn(Optional.of(destinationDevice));

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
      when(accountsManager.getByServiceIdentifier(serviceIdentifier)).thenReturn(Optional.of(destinationAccount));

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages =
          Map.of(deviceId, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(registrationId)
              .setPayload(ByteString.copyFrom(TestRandomUtil.nextBytes(128)))
              .build());

      final SendMessageResponse response = SendMessageResponse.newBuilder()
          .setChallengeRequired(ChallengeRequired.newBuilder()
              .addChallengeOptions(ChallengeRequired.ChallengeType.CAPTCHA))
          .build();

      when(spamChecker.checkForIndividualRecipientSpamGrpc(any(), any(), any(), any()))
          .thenReturn(new SpamCheckResult<>(Optional.of(GrpcResponse.withResponse(response)), Optional.empty()));

      assertEquals(response, authenticatedServiceStub().sendMessage(
          generateRequest(serviceIdentifier, AuthenticatedSenderMessageType.DOUBLE_RATCHET, false, true, messages)));

      verify(spamChecker).checkForIndividualRecipientSpamGrpc(MessageType.INDIVIDUAL_IDENTIFIED_SENDER,
          Optional.of(new AuthenticatedDevice(AUTHENTICATED_ACI, AUTHENTICATED_DEVICE_ID)),
          Optional.of(destinationAccount),
          serviceIdentifier);

      verify(messageSender, never()).sendMessages(any(), any(), any(), any(), any(), any());
    }

    private static SendAuthenticatedSenderMessageRequest generateRequest(final ServiceIdentifier serviceIdentifier,
        final AuthenticatedSenderMessageType messageType,
        final boolean ephemeral,
        final boolean urgent,
        final Map<Byte, IndividualRecipientMessageBundle.Message> messages) {

      final IndividualRecipientMessageBundle.Builder messageBundleBuilder = IndividualRecipientMessageBundle.newBuilder()
          .setTimestamp(CLOCK.millis());

      messages.forEach(messageBundleBuilder::putMessages);

      final SendAuthenticatedSenderMessageRequest.Builder requestBuilder = SendAuthenticatedSenderMessageRequest.newBuilder()
          .setDestination(ServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier))
          .setType(messageType)
          .setMessages(messageBundleBuilder)
          .setEphemeral(ephemeral)
          .setUrgent(urgent);

      return requestBuilder.build();
    }
  }

  @Nested
  class Sync {

    @CartesianTest
    void sendMessage(@CartesianTest.Enum(mode = CartesianTest.Enum.Mode.EXCLUDE, names = {"UNSPECIFIED", "UNRECOGNIZED"}) final AuthenticatedSenderMessageType messageType,
        @CartesianTest.Values(booleans = {true, false}) final boolean urgent,
        @CartesianTest.Values(booleans = {true, false}) final boolean includeReportSpamToken)
        throws MessageTooLargeException, MismatchedDevicesException {

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(AUTHENTICATED_ACI);
      final byte[] payload = TestRandomUtil.nextBytes(128);

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages =
          Map.of(LINKED_DEVICE_ID, IndividualRecipientMessageBundle.Message.newBuilder()
                  .setRegistrationId(LINKED_DEVICE_REGISTRATION_ID)
                  .setPayload(ByteString.copyFrom(payload))
                  .build(),

              SECOND_LINKED_DEVICE_ID, IndividualRecipientMessageBundle.Message.newBuilder()
                  .setRegistrationId(SECOND_LINKED_DEVICE_REGISTRATION_ID)
                  .setPayload(ByteString.copyFrom(payload))
                  .build());

      final byte[] reportSpamToken = TestRandomUtil.nextBytes(64);

      if (includeReportSpamToken) {
        when(spamChecker.checkForIndividualRecipientSpamGrpc(any(), any(), any(), any()))
            .thenReturn(new SpamCheckResult<>(Optional.empty(), Optional.of(reportSpamToken)));
      }

      final SendMessageResponse response =
          authenticatedServiceStub().sendSyncMessage(generateRequest(messageType, urgent, messages));

      assertEquals(SendMessageResponse.newBuilder().build(), response);

      final MessageProtos.Envelope.Type expectedEnvelopeType = switch (messageType) {
        case DOUBLE_RATCHET -> MessageProtos.Envelope.Type.CIPHERTEXT;
        case PREKEY_MESSAGE -> MessageProtos.Envelope.Type.PREKEY_BUNDLE;
        case PLAINTEXT_CONTENT -> MessageProtos.Envelope.Type.PLAINTEXT_CONTENT;
        case UNSPECIFIED, UNRECOGNIZED -> throw new IllegalArgumentException("Unexpected message type: " + messageType);
      };

      final Map<Byte, MessageProtos.Envelope> expectedEnvelopes = new HashMap<>(Map.of(
          LINKED_DEVICE_ID, MessageProtos.Envelope.newBuilder()
              .setType(expectedEnvelopeType)
              .setSourceServiceId(serviceIdentifier.toServiceIdentifierString())
              .setSourceDevice(AUTHENTICATED_DEVICE_ID)
              .setDestinationServiceId(serviceIdentifier.toServiceIdentifierString())
              .setClientTimestamp(CLOCK.millis())
              .setServerTimestamp(CLOCK.millis())
              .setEphemeral(false)
              .setUrgent(urgent)
              .setContent(ByteString.copyFrom(payload))
              .build(),

          SECOND_LINKED_DEVICE_ID, MessageProtos.Envelope.newBuilder()
              .setType(expectedEnvelopeType)
              .setSourceServiceId(serviceIdentifier.toServiceIdentifierString())
              .setSourceDevice(AUTHENTICATED_DEVICE_ID)
              .setDestinationServiceId(serviceIdentifier.toServiceIdentifierString())
              .setClientTimestamp(CLOCK.millis())
              .setServerTimestamp(CLOCK.millis())
              .setEphemeral(false)
              .setUrgent(urgent)
              .setContent(ByteString.copyFrom(payload))
              .build()
      ));

      if (includeReportSpamToken) {
        expectedEnvelopes.replaceAll((deviceId, envelope) ->
            envelope.toBuilder().setReportSpamToken(ByteString.copyFrom(reportSpamToken)).build());
      }

      verify(spamChecker).checkForIndividualRecipientSpamGrpc(MessageType.SYNC,
          Optional.of(new AuthenticatedDevice(AUTHENTICATED_ACI, AUTHENTICATED_DEVICE_ID)),
          Optional.of(authenticatedAccount),
          serviceIdentifier);

      verify(messageSender).sendMessages(authenticatedAccount,
          serviceIdentifier,
          expectedEnvelopes,
          Map.of(LINKED_DEVICE_ID, LINKED_DEVICE_REGISTRATION_ID,
              SECOND_LINKED_DEVICE_ID, SECOND_LINKED_DEVICE_REGISTRATION_ID),
          Optional.of(AUTHENTICATED_DEVICE_ID),
          null);
    }

    @Test
    void mismatchedDevices() throws MessageTooLargeException, MismatchedDevicesException {
      final byte missingDeviceId = Device.PRIMARY_ID;
      final byte extraDeviceId = missingDeviceId + 1;
      final byte staleDeviceId = extraDeviceId + 1;

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages = Map.of(
          staleDeviceId, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(Device.PRIMARY_ID)
              .setPayload(ByteString.copyFrom(TestRandomUtil.nextBytes(128)))
              .build());

      doThrow(new MismatchedDevicesException(new org.whispersystems.textsecuregcm.controllers.MismatchedDevices(
          Set.of(missingDeviceId), Set.of(extraDeviceId), Set.of(staleDeviceId))))
          .when(messageSender).sendMessages(any(), any(), any(), any(), any(), any());

      final SendMessageResponse response = authenticatedServiceStub().sendSyncMessage(
          generateRequest(AuthenticatedSenderMessageType.DOUBLE_RATCHET, true, messages));

      final SendMessageResponse expectedResponse = SendMessageResponse.newBuilder()
          .setMismatchedDevices(MismatchedDevices.newBuilder()
              .setServiceIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(AUTHENTICATED_ACI)))
              .addMissingDevices(missingDeviceId)
              .addStaleDevices(staleDeviceId)
              .addExtraDevices(extraDeviceId)
              .build())
          .build();

      assertEquals(expectedResponse, response);
    }

    @Test
    void rateLimited() throws RateLimitExceededException, MessageTooLargeException, MismatchedDevicesException {
      final Duration retryDuration = Duration.ofHours(7);
      doThrow(new RateLimitExceededException(retryDuration))
          .when(rateLimiter).validate(eq(AUTHENTICATED_ACI), anyInt());

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages =
          Map.of(AUTHENTICATED_DEVICE_ID, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(AUTHENTICATED_REGISTRATION_ID)
              .setPayload(ByteString.copyFrom(TestRandomUtil.nextBytes(128)))
              .build());

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertRateLimitExceeded(retryDuration,
          () -> authenticatedServiceStub().sendSyncMessage(
              generateRequest(AuthenticatedSenderMessageType.DOUBLE_RATCHET, true, messages)));

      verify(messageSender, never()).sendMessages(any(), any(), any(), any(), any(), any());
      verify(messageByteLimitEstimator).add(AUTHENTICATED_ACI.toString());
    }

    @Test
    void oversizedMessage() throws MessageTooLargeException, MismatchedDevicesException {
      final byte missingDeviceId = Device.PRIMARY_ID;
      final byte extraDeviceId = missingDeviceId + 1;
      final byte staleDeviceId = extraDeviceId + 1;

      final Account destinationAccount = mock(Account.class);

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
      when(accountsManager.getByServiceIdentifier(serviceIdentifier)).thenReturn(Optional.of(destinationAccount));

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages = Map.of(
          staleDeviceId, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(Device.PRIMARY_ID)
              .setPayload(ByteString.copyFrom(TestRandomUtil.nextBytes(128)))
              .build());

      doThrow(new MessageTooLargeException())
          .when(messageSender).sendMessages(any(), any(), any(), any(), any(), any());

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
          () -> authenticatedServiceStub().sendSyncMessage(
              generateRequest(AuthenticatedSenderMessageType.DOUBLE_RATCHET, true, messages)));
    }

    private static SendSyncMessageRequest generateRequest(final AuthenticatedSenderMessageType messageType,
        final boolean urgent,
        final Map<Byte, IndividualRecipientMessageBundle.Message> messages) {

      final IndividualRecipientMessageBundle.Builder messageBundleBuilder = IndividualRecipientMessageBundle.newBuilder()
          .setTimestamp(CLOCK.millis());

      messages.forEach(messageBundleBuilder::putMessages);

      final SendSyncMessageRequest.Builder requestBuilder = SendSyncMessageRequest.newBuilder()
          .setType(messageType)
          .setMessages(messageBundleBuilder)
          .setUrgent(urgent);

      return requestBuilder.build();
    }
  }
}
