/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.mockito.Mock;
import org.signal.chat.messages.ChallengeRequired;
import org.signal.chat.messages.IndividualRecipientMessageBundle;
import org.signal.chat.messages.MessagesAnonymousGrpc;
import org.signal.chat.messages.MismatchedDevices;
import org.signal.chat.messages.MultiRecipientMessage;
import org.signal.chat.messages.MultiRecipientMismatchedDevices;
import org.signal.chat.messages.SendMessageResponse;
import org.signal.chat.messages.SendMultiRecipientMessageRequest;
import org.signal.chat.messages.SendMultiRecipientMessageResponse;
import org.signal.chat.messages.SendMultiRecipientStoryRequest;
import org.signal.chat.messages.SendSealedSenderMessageRequest;
import org.signal.chat.messages.SendStoryMessageRequest;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.MultiRecipientMismatchedDevicesException;
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
import org.whispersystems.textsecuregcm.tests.util.MultiRecipientMessageHelper;
import org.whispersystems.textsecuregcm.tests.util.TestRecipient;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class MessagesAnonymousGrpcServiceTest extends
    SimpleBaseGrpcTest<MessagesAnonymousGrpcService, MessagesAnonymousGrpc.MessagesAnonymousBlockingStub> {

  @Mock
  private AccountsManager accountsManager;

  @Mock
  private RateLimiters rateLimiters;

  @Mock
  private MessageSender messageSender;

  @Mock
  private GroupSendTokenUtil groupSendTokenUtil;

  @Mock
  private CardinalityEstimator messageByteLimitEstimator;

  @Mock
  private SpamChecker spamChecker;

  @Mock
  private RateLimiter rateLimiter;

  private static final TestClock CLOCK = TestClock.pinned(Instant.now());

  private static final byte[] UNIDENTIFIED_ACCESS_KEY =
      TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

  private static final byte[] GROUP_SEND_TOKEN = TestRandomUtil.nextBytes(64);

  @Override
  protected MessagesAnonymousGrpcService createServiceBeforeEachTest() {
    return new MessagesAnonymousGrpcService(accountsManager,
        rateLimiters,
        messageSender,
        groupSendTokenUtil,
        messageByteLimitEstimator,
        spamChecker,
        CLOCK);
  }

  @BeforeEach
  void setUp() throws StatusException {
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.empty());
    when(accountsManager.getByServiceIdentifierAsync(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    when(rateLimiters.getInboundMessageBytes()).thenReturn(rateLimiter);
    when(rateLimiters.getStoriesLimiter()).thenReturn(rateLimiter);

    doThrow(Status.UNAUTHENTICATED.asException()).when(groupSendTokenUtil)
        .checkGroupSendToken(any(), any(ServiceIdentifier.class));

    doThrow(Status.UNAUTHENTICATED.asException()).when(groupSendTokenUtil)
        .checkGroupSendToken(any(), anyCollection());

    doAnswer(invocation -> null).when(groupSendTokenUtil)
        .checkGroupSendToken(eq(ByteString.copyFrom(GROUP_SEND_TOKEN)), any(ServiceIdentifier.class));

    doAnswer(invocation -> null).when(groupSendTokenUtil)
        .checkGroupSendToken(eq(ByteString.copyFrom(GROUP_SEND_TOKEN)), anyCollection());

    when(spamChecker.checkForIndividualRecipientSpamGrpc(any(), any(), any(), any()))
        .thenReturn(new SpamCheckResult<>(Optional.empty(), Optional.empty()));

    when(spamChecker.checkForMultiRecipientSpamGrpc(any()))
        .thenReturn(new SpamCheckResult<>(Optional.empty(), Optional.empty()));
  }

  @Nested
  class SingleRecipient {

    @CartesianTest
    void sendMessage(@CartesianTest.Values(booleans = {true, false}) final boolean useUak,
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
      when(destinationAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(UNIDENTIFIED_ACCESS_KEY));

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
      when(accountsManager.getByServiceIdentifier(serviceIdentifier)).thenReturn(Optional.of(destinationAccount));

      final byte[] payload = TestRandomUtil.nextBytes(128);

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages =
          Map.of(deviceId, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(registrationId)
              .setPayload(ByteString.copyFrom(payload))
              .build());

      final byte[] reportSpamToken = TestRandomUtil.nextBytes(64);

      if (includeReportSpamToken) {
        when(spamChecker.checkForIndividualRecipientSpamGrpc(any(), any(), any(), any()))
            .thenReturn(new SpamCheckResult<>(Optional.empty(), Optional.of(reportSpamToken)));
      }

      final SendMessageResponse response = unauthenticatedServiceStub().sendSingleRecipientMessage(
          generateRequest(serviceIdentifier, ephemeral, urgent, messages,
              useUak ? UNIDENTIFIED_ACCESS_KEY : null,
              useUak ? null : GROUP_SEND_TOKEN));

      assertEquals(SendMessageResponse.newBuilder().build(), response);

      final MessageProtos.Envelope.Builder expectedEnvelopeBuilder = MessageProtos.Envelope.newBuilder()
          .setType(MessageProtos.Envelope.Type.UNIDENTIFIED_SENDER)
          .setDestinationServiceId(serviceIdentifier.toServiceIdentifierString())
          .setClientTimestamp(CLOCK.millis())
          .setServerTimestamp(CLOCK.millis())
          .setEphemeral(ephemeral)
          .setUrgent(urgent)
          .setStory(false)
          .setContent(ByteString.copyFrom(payload));

      if (includeReportSpamToken) {
        expectedEnvelopeBuilder.setReportSpamToken(ByteString.copyFrom(reportSpamToken));
      }

      verify(spamChecker).checkForIndividualRecipientSpamGrpc(MessageType.INDIVIDUAL_SEALED_SENDER,
          Optional.empty(),
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
      when(destinationAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(UNIDENTIFIED_ACCESS_KEY));

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

      final SendMessageResponse response = unauthenticatedServiceStub().sendSingleRecipientMessage(
          generateRequest(serviceIdentifier, false, true, messages, UNIDENTIFIED_ACCESS_KEY, null));

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

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void badCredentials(final boolean useUak) throws MessageTooLargeException, MismatchedDevicesException {
      final byte deviceId = Device.PRIMARY_ID;
      final int registrationId = 7;

      final Device destinationDevice = DevicesHelper.createDevice(deviceId, CLOCK.millis(), registrationId);

      final Account destinationAccount = mock(Account.class);
      when(destinationAccount.getDevices()).thenReturn(List.of(destinationDevice));
      when(destinationAccount.getDevice(deviceId)).thenReturn(Optional.of(destinationDevice));
      when(destinationAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(UNIDENTIFIED_ACCESS_KEY));

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
      when(accountsManager.getByServiceIdentifier(serviceIdentifier)).thenReturn(Optional.of(destinationAccount));

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages =
          Map.of(deviceId, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(registrationId)
              .setPayload(ByteString.copyFrom(TestRandomUtil.nextBytes(128)))
              .build());

      final byte[] incorrectUnidentifiedAccessKey = UNIDENTIFIED_ACCESS_KEY.clone();
      incorrectUnidentifiedAccessKey[0] += 1;

      final byte[] incorrectGroupSendToken = GROUP_SEND_TOKEN.clone();
      incorrectGroupSendToken[0] += 1;

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusException(Status.UNAUTHENTICATED,
          () -> unauthenticatedServiceStub().sendSingleRecipientMessage(
              generateRequest(serviceIdentifier, false, true, messages,
                  useUak ? incorrectUnidentifiedAccessKey : null,
                  useUak ? null : incorrectGroupSendToken)));

      verify(messageSender, never()).sendMessages(any(), any(), any(), any(), any(), any());
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
      GrpcTestUtils.assertStatusException(Status.UNAUTHENTICATED,
          () -> unauthenticatedServiceStub().sendSingleRecipientMessage(
              generateRequest(serviceIdentifier, false, true, messages, UNIDENTIFIED_ACCESS_KEY, null)));

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
      when(destinationAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(UNIDENTIFIED_ACCESS_KEY));

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
      when(accountsManager.getByServiceIdentifier(serviceIdentifier)).thenReturn(Optional.of(destinationAccount));

      final Duration retryDuration = Duration.ofHours(7);

      doThrow(new RateLimitExceededException(retryDuration)).when(rateLimiter).validate(eq(serviceIdentifier.uuid()), anyInt());

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages =
          Map.of(deviceId, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(registrationId)
              .setPayload(ByteString.copyFrom(TestRandomUtil.nextBytes(128)))
              .build());

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertRateLimitExceeded(retryDuration,
          () -> unauthenticatedServiceStub().sendSingleRecipientMessage(
              generateRequest(serviceIdentifier, false, true, messages, UNIDENTIFIED_ACCESS_KEY, null)));

      verify(messageSender, never()).sendMessages(any(), any(), any(), any(), any(), any());
      verify(messageByteLimitEstimator).add(serviceIdentifier.uuid().toString());
    }

    @Test
    void oversizedMessage() throws MessageTooLargeException, MismatchedDevicesException {
      final byte missingDeviceId = Device.PRIMARY_ID;
      final byte extraDeviceId = missingDeviceId + 1;
      final byte staleDeviceId = extraDeviceId + 1;

      final Account destinationAccount = mock(Account.class);
      when(destinationAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(UNIDENTIFIED_ACCESS_KEY));

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
          () -> unauthenticatedServiceStub().sendSingleRecipientMessage(
              generateRequest(serviceIdentifier, false, true, messages, UNIDENTIFIED_ACCESS_KEY, null)));
    }

    @Test
    void spamWithStatus() throws MessageTooLargeException, MismatchedDevicesException {
      final byte deviceId = Device.PRIMARY_ID;
      final int registrationId = 7;

      final Device destinationDevice = DevicesHelper.createDevice(deviceId, CLOCK.millis(), registrationId);

      final Account destinationAccount = mock(Account.class);
      when(destinationAccount.getDevices()).thenReturn(List.of(destinationDevice));
      when(destinationAccount.getDevice(deviceId)).thenReturn(Optional.of(destinationDevice));
      when(destinationAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(UNIDENTIFIED_ACCESS_KEY));

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
          () -> unauthenticatedServiceStub().sendSingleRecipientMessage(
              generateRequest(serviceIdentifier, false, true, messages, UNIDENTIFIED_ACCESS_KEY, null)));

      verify(spamChecker).checkForIndividualRecipientSpamGrpc(MessageType.INDIVIDUAL_SEALED_SENDER,
          Optional.empty(),
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
      when(destinationAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(UNIDENTIFIED_ACCESS_KEY));

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

      assertEquals(response, unauthenticatedServiceStub().sendSingleRecipientMessage(
          generateRequest(serviceIdentifier, false, true, messages, UNIDENTIFIED_ACCESS_KEY, null)));

      verify(spamChecker).checkForIndividualRecipientSpamGrpc(MessageType.INDIVIDUAL_SEALED_SENDER,
          Optional.empty(),
          Optional.of(destinationAccount),
          serviceIdentifier);

      verify(messageSender, never()).sendMessages(any(), any(), any(), any(), any(), any());
    }

    private static SendSealedSenderMessageRequest generateRequest(final ServiceIdentifier serviceIdentifier,
        final boolean ephemeral,
        final boolean urgent,
        final Map<Byte, IndividualRecipientMessageBundle.Message> messages,
        @Nullable final byte[] unidentifiedAccessKey,
        @Nullable final byte[] groupSendToken) {

      final IndividualRecipientMessageBundle.Builder messageBundleBuilder = IndividualRecipientMessageBundle.newBuilder()
          .setTimestamp(CLOCK.millis());

      messages.forEach(messageBundleBuilder::putMessages);

      final SendSealedSenderMessageRequest.Builder requestBuilder = SendSealedSenderMessageRequest.newBuilder()
          .setDestination(ServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier))
          .setMessages(messageBundleBuilder)
          .setEphemeral(ephemeral)
          .setUrgent(urgent);

      if (unidentifiedAccessKey != null) {
        requestBuilder.setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey));
      }

      if (groupSendToken != null) {
        requestBuilder.setGroupSendToken(ByteString.copyFrom(groupSendToken));
      }

      return requestBuilder.build();
    }
  }

  @Nested
  class MultiRecipient {

    @CartesianTest
    void sendMessage(@CartesianTest.Values(booleans = {true, false}) final boolean ephemeral,
        @CartesianTest.Values(booleans = {true, false}) final boolean urgent)
        throws MessageTooLargeException, MultiRecipientMismatchedDevicesException {

      final byte deviceId = Device.PRIMARY_ID;
      final int registrationId = 7;

      final Device destinationDevice = DevicesHelper.createDevice(deviceId, CLOCK.millis(), registrationId);

      final Account resolvedAccount = mock(Account.class);
      when(resolvedAccount.getDevices()).thenReturn(List.of(destinationDevice));
      when(resolvedAccount.getDevice(deviceId)).thenReturn(Optional.of(destinationDevice));

      final AciServiceIdentifier resolvedServiceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
      final AciServiceIdentifier unresolvedServiceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

      when(accountsManager.getByServiceIdentifierAsync(resolvedServiceIdentifier))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(resolvedAccount)));

      final TestRecipient resolvedRecipient =
          new TestRecipient(resolvedServiceIdentifier, deviceId, registrationId, new byte[48]);

      final TestRecipient unresolvedRecipient =
          new TestRecipient(unresolvedServiceIdentifier, Device.PRIMARY_ID, 1, new byte[48]);

      final byte[] payload = MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(
          resolvedRecipient, unresolvedRecipient));

      final SendMultiRecipientMessageRequest request = SendMultiRecipientMessageRequest.newBuilder()
          .setGroupSendToken(ByteString.copyFrom(GROUP_SEND_TOKEN))
          .setMessage(MultiRecipientMessage.newBuilder()
              .setTimestamp(CLOCK.millis())
              .setPayload(ByteString.copyFrom(payload))
              .build())
          .setEphemeral(ephemeral)
          .setUrgent(urgent)
          .build();

      final SendMultiRecipientMessageResponse response =
          unauthenticatedServiceStub().sendMultiRecipientMessage(request);

      final SendMultiRecipientMessageResponse expectedResponse = SendMultiRecipientMessageResponse.newBuilder()
          .addUnresolvedRecipients(ServiceIdentifierUtil.toGrpcServiceIdentifier(unresolvedServiceIdentifier))
          .build();

      assertEquals(expectedResponse, response);

      verify(messageSender).sendMultiRecipientMessage(any(),
          argThat(resolvedRecipients -> resolvedRecipients.containsValue(resolvedAccount)),
          eq(CLOCK.millis()),
          eq(false),
          eq(ephemeral),
          eq(urgent),
          any());
    }

    @Test
    void mismatchedDevices() throws MessageTooLargeException, MultiRecipientMismatchedDevicesException {
      final byte missingDeviceId = Device.PRIMARY_ID;
      final byte extraDeviceId = missingDeviceId + 1;
      final byte staleDeviceId = extraDeviceId + 1;

      final Account destinationAccount = mock(Account.class);

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

      when(accountsManager.getByServiceIdentifierAsync(serviceIdentifier))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(destinationAccount)));

      final byte[] payload = MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(
          new TestRecipient(serviceIdentifier, staleDeviceId, 17, new byte[48])));

      final SendMultiRecipientMessageRequest request = SendMultiRecipientMessageRequest.newBuilder()
          .setGroupSendToken(ByteString.copyFrom(GROUP_SEND_TOKEN))
          .setMessage(MultiRecipientMessage.newBuilder()
              .setTimestamp(CLOCK.millis())
              .setPayload(ByteString.copyFrom(payload))
              .build())
          .setEphemeral(false)
          .setUrgent(true)
          .build();

      doThrow(new MultiRecipientMismatchedDevicesException(Map.of(serviceIdentifier,
          new org.whispersystems.textsecuregcm.controllers.MismatchedDevices(
              Set.of(missingDeviceId), Set.of(extraDeviceId), Set.of(staleDeviceId)))))
          .when(messageSender).sendMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean(), any());

      final SendMultiRecipientMessageResponse response =
          unauthenticatedServiceStub().sendMultiRecipientMessage(request);

      final SendMultiRecipientMessageResponse expectedResponse = SendMultiRecipientMessageResponse.newBuilder()
          .setMismatchedDevices(MultiRecipientMismatchedDevices.newBuilder()
              .addMismatchedDevices(MismatchedDevices.newBuilder()
                  .setServiceIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier))
                  .addMissingDevices(missingDeviceId)
                  .addExtraDevices(extraDeviceId)
                  .addStaleDevices(staleDeviceId)
                  .build())
              .build())
          .build();

      assertEquals(expectedResponse, response);
    }

    @Test
    void badCredentials() throws MessageTooLargeException, MultiRecipientMismatchedDevicesException {
      final byte deviceId = Device.PRIMARY_ID;
      final int registrationId = 7;

      final Device destinationDevice = DevicesHelper.createDevice(deviceId, CLOCK.millis(), registrationId);

      final Account destinationAccount = mock(Account.class);
      when(destinationAccount.getDevices()).thenReturn(List.of(destinationDevice));
      when(destinationAccount.getDevice(deviceId)).thenReturn(Optional.of(destinationDevice));

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

      when(accountsManager.getByServiceIdentifierAsync(serviceIdentifier))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(destinationAccount)));

      final TestRecipient recipient = new TestRecipient(serviceIdentifier, deviceId, registrationId, new byte[48]);
      final byte[] payload = MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(recipient));

      final byte[] incorrectGroupSendToken = GROUP_SEND_TOKEN.clone();
      incorrectGroupSendToken[0] += 1;

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusException(Status.UNAUTHENTICATED, () ->
          unauthenticatedServiceStub().sendMultiRecipientMessage(SendMultiRecipientMessageRequest.newBuilder()
              .setGroupSendToken(ByteString.copyFrom(incorrectGroupSendToken))
              .setMessage(MultiRecipientMessage.newBuilder()
                  .setTimestamp(CLOCK.millis())
                  .setPayload(ByteString.copyFrom(payload))
                  .build())
              .setEphemeral(false)
              .setUrgent(true)
              .build()));

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusException(Status.UNAUTHENTICATED, () ->
          unauthenticatedServiceStub().sendMultiRecipientMessage(SendMultiRecipientMessageRequest.newBuilder()
              .setMessage(MultiRecipientMessage.newBuilder()
                  .setTimestamp(CLOCK.millis())
                  .setPayload(ByteString.copyFrom(payload))
                  .build())
              .setEphemeral(false)
              .setUrgent(true)
              .build()));

      verify(messageSender, never())
          .sendMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean(), any());
    }

    @Test
    void badPayload() throws MessageTooLargeException, MultiRecipientMismatchedDevicesException {
      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT, () ->
          unauthenticatedServiceStub().sendMultiRecipientMessage(SendMultiRecipientMessageRequest.newBuilder()
              .setMessage(MultiRecipientMessage.newBuilder()
                  .setTimestamp(CLOCK.millis())
                  .setPayload(ByteString.copyFrom(TestRandomUtil.nextBytes(128)))
                  .build())
              .build()));

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT, () ->
          unauthenticatedServiceStub().sendMultiRecipientMessage(SendMultiRecipientMessageRequest.newBuilder().build()));

      verify(messageSender, never())
          .sendMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean(), any());
    }

    @Test
    void repeatedRecipient() throws MessageTooLargeException, MultiRecipientMismatchedDevicesException {
      final Device destinationDevice = DevicesHelper.createDevice(Device.PRIMARY_ID, CLOCK.millis(), 1);

      final Account destinationAccount = mock(Account.class);
      when(destinationAccount.getDevices()).thenReturn(List.of(destinationDevice));
      when(destinationAccount.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(destinationDevice));

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

      when(accountsManager.getByServiceIdentifierAsync(serviceIdentifier))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(destinationAccount)));

      final TestRecipient recipient = new TestRecipient(serviceIdentifier, Device.PRIMARY_ID, 1, new byte[48]);

      final byte[] payload = MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(recipient, recipient));

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT, () ->
          unauthenticatedServiceStub().sendMultiRecipientMessage(SendMultiRecipientMessageRequest.newBuilder()
              .setGroupSendToken(ByteString.copyFrom(GROUP_SEND_TOKEN))
              .setMessage(MultiRecipientMessage.newBuilder()
                  .setTimestamp(CLOCK.millis())
                  .setPayload(ByteString.copyFrom(payload))
                  .build())
              .setEphemeral(false)
              .setUrgent(true)
              .build()));

      verify(messageSender, never())
          .sendMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean(), any());
    }

    @Test
    void oversizedMessage() throws MessageTooLargeException, MultiRecipientMismatchedDevicesException {
      final Account destinationAccount = mock(Account.class);

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

      when(accountsManager.getByServiceIdentifierAsync(serviceIdentifier))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(destinationAccount)));

      final byte[] payload = MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(
          new TestRecipient(serviceIdentifier, Device.PRIMARY_ID, 17, new byte[48])));

      final SendMultiRecipientMessageRequest request = SendMultiRecipientMessageRequest.newBuilder()
          .setGroupSendToken(ByteString.copyFrom(GROUP_SEND_TOKEN))
          .setMessage(MultiRecipientMessage.newBuilder()
              .setTimestamp(CLOCK.millis())
              .setPayload(ByteString.copyFrom(payload))
              .build())
          .setEphemeral(false)
          .setUrgent(true)
          .build();

      doThrow(new MessageTooLargeException())
          .when(messageSender).sendMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean(), any());

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
          () -> unauthenticatedServiceStub().sendMultiRecipientMessage(request));
    }

    @Test
    void spamWithStatus() throws MessageTooLargeException, MultiRecipientMismatchedDevicesException {
      final byte deviceId = Device.PRIMARY_ID;
      final int registrationId = 7;

      final Device destinationDevice = DevicesHelper.createDevice(deviceId, CLOCK.millis(), registrationId);

      final Account destinationAccount = mock(Account.class);
      when(destinationAccount.getDevices()).thenReturn(List.of(destinationDevice));
      when(destinationAccount.getDevice(deviceId)).thenReturn(Optional.of(destinationDevice));

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

      when(accountsManager.getByServiceIdentifierAsync(serviceIdentifier))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(destinationAccount)));

      final TestRecipient recipient =
          new TestRecipient(serviceIdentifier, deviceId, registrationId, new byte[48]);

      final byte[] payload = MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(recipient));

      final SendMultiRecipientMessageRequest request = SendMultiRecipientMessageRequest.newBuilder()
          .setGroupSendToken(ByteString.copyFrom(GROUP_SEND_TOKEN))
          .setMessage(MultiRecipientMessage.newBuilder()
              .setTimestamp(CLOCK.millis())
              .setPayload(ByteString.copyFrom(payload))
              .build())
          .setEphemeral(false)
          .setUrgent(true)
          .build();

      when(spamChecker.checkForMultiRecipientSpamGrpc(any()))
          .thenReturn(new SpamCheckResult<>(Optional.of(GrpcResponse.withStatus(Status.RESOURCE_EXHAUSTED)), Optional.empty()));

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusException(Status.RESOURCE_EXHAUSTED,
          () -> unauthenticatedServiceStub().sendMultiRecipientMessage(request));

      verify(spamChecker).checkForMultiRecipientSpamGrpc(MessageType.MULTI_RECIPIENT_SEALED_SENDER);

      verify(messageSender, never())
          .sendMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean(), any());
    }

    @Test
    void spamWithResponse() throws MessageTooLargeException, MultiRecipientMismatchedDevicesException {
      final byte deviceId = Device.PRIMARY_ID;
      final int registrationId = 7;

      final Device destinationDevice = DevicesHelper.createDevice(deviceId, CLOCK.millis(), registrationId);

      final Account destinationAccount = mock(Account.class);
      when(destinationAccount.getDevices()).thenReturn(List.of(destinationDevice));
      when(destinationAccount.getDevice(deviceId)).thenReturn(Optional.of(destinationDevice));

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

      when(accountsManager.getByServiceIdentifierAsync(serviceIdentifier))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(destinationAccount)));

      final TestRecipient recipient =
          new TestRecipient(serviceIdentifier, deviceId, registrationId, new byte[48]);

      final byte[] payload = MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(recipient));

      final SendMultiRecipientMessageRequest request = SendMultiRecipientMessageRequest.newBuilder()
          .setGroupSendToken(ByteString.copyFrom(GROUP_SEND_TOKEN))
          .setMessage(MultiRecipientMessage.newBuilder()
              .setTimestamp(CLOCK.millis())
              .setPayload(ByteString.copyFrom(payload))
              .build())
          .setEphemeral(false)
          .setUrgent(true)
          .build();

      final SendMultiRecipientMessageResponse response = SendMultiRecipientMessageResponse.newBuilder()
          .setChallengeRequired(ChallengeRequired.newBuilder()
              .addChallengeOptions(ChallengeRequired.ChallengeType.CAPTCHA))
          .build();

      when(spamChecker.checkForMultiRecipientSpamGrpc(any()))
          .thenReturn(new SpamCheckResult<>(Optional.of(GrpcResponse.withResponse(response)), Optional.empty()));

      assertEquals(response, unauthenticatedServiceStub().sendMultiRecipientMessage(request));

      verify(spamChecker).checkForMultiRecipientSpamGrpc(MessageType.MULTI_RECIPIENT_SEALED_SENDER);

      verify(messageSender, never())
          .sendMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean(), any());
    }
  }

  @Nested
  class SingleRecipientStory {

    @CartesianTest
    void sendStory(@CartesianTest.Values(booleans = {true, false}) final boolean urgent,
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

      final byte[] payload = TestRandomUtil.nextBytes(128);

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages =
          Map.of(deviceId, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(registrationId)
              .setPayload(ByteString.copyFrom(payload))
              .build());

      final byte[] reportSpamToken = TestRandomUtil.nextBytes(64);

      if (includeReportSpamToken) {
        when(spamChecker.checkForIndividualRecipientSpamGrpc(any(), any(), any(), any()))
            .thenReturn(new SpamCheckResult<>(Optional.empty(), Optional.of(reportSpamToken)));
      }

      final SendMessageResponse response =
          unauthenticatedServiceStub().sendStory(generateRequest(serviceIdentifier, urgent, messages));

      assertEquals(SendMessageResponse.newBuilder().build(), response);

      final MessageProtos.Envelope.Builder expectedEnvelopeBuilder = MessageProtos.Envelope.newBuilder()
          .setType(MessageProtos.Envelope.Type.UNIDENTIFIED_SENDER)
          .setDestinationServiceId(serviceIdentifier.toServiceIdentifierString())
          .setClientTimestamp(CLOCK.millis())
          .setServerTimestamp(CLOCK.millis())
          .setEphemeral(false)
          .setUrgent(urgent)
          .setStory(true)
          .setContent(ByteString.copyFrom(payload));

      if (includeReportSpamToken) {
        expectedEnvelopeBuilder.setReportSpamToken(ByteString.copyFrom(reportSpamToken));
      }

      verify(spamChecker).checkForIndividualRecipientSpamGrpc(MessageType.INDIVIDUAL_STORY,
          Optional.empty(),
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
      when(destinationAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(UNIDENTIFIED_ACCESS_KEY));

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

      final SendMessageResponse response = unauthenticatedServiceStub().sendStory(
          generateRequest(serviceIdentifier, false, messages));

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
      final Map<Byte, IndividualRecipientMessageBundle.Message> messages =
          Map.of(Device.PRIMARY_ID, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(7)
              .setPayload(ByteString.copyFrom(TestRandomUtil.nextBytes(128)))
              .build());

      final SendMessageResponse response = unauthenticatedServiceStub().sendStory(
          generateRequest(new AciServiceIdentifier(UUID.randomUUID()), true, messages));

      assertEquals(SendMessageResponse.newBuilder().build(), response);

      verify(messageSender, never()).sendMessages(any(), any(), any(), any(), any(), any());
    }

    @Test
    void rateLimited() throws RateLimitExceededException, MessageTooLargeException, MismatchedDevicesException {
      final byte deviceId = Device.PRIMARY_ID;
      final int registrationId = 7;

      final Device destinationDevice = DevicesHelper.createDevice(deviceId, CLOCK.millis(), registrationId);
      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

      final Account destinationAccount = mock(Account.class);
      when(destinationAccount.getDevices()).thenReturn(List.of(destinationDevice));
      when(destinationAccount.getDevice(deviceId)).thenReturn(Optional.of(destinationDevice));
      when(destinationAccount.getIdentifier(IdentityType.ACI)).thenReturn(serviceIdentifier.uuid());

      when(accountsManager.getByServiceIdentifier(serviceIdentifier)).thenReturn(Optional.of(destinationAccount));

      final Duration retryDuration = Duration.ofHours(7);
      doThrow(new RateLimitExceededException(retryDuration)).when(rateLimiter).validate(eq(serviceIdentifier.uuid()));

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages =
          Map.of(deviceId, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(registrationId)
              .setPayload(ByteString.copyFrom(TestRandomUtil.nextBytes(128)))
              .build());

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertRateLimitExceeded(retryDuration,
          () -> unauthenticatedServiceStub().sendStory(generateRequest(serviceIdentifier, true, messages)));

      verify(messageSender, never()).sendMessages(any(), any(), any(), any(), any(), any());
    }

    @Test
    void oversizedMessage() throws MessageTooLargeException, MismatchedDevicesException {
      final byte missingDeviceId = Device.PRIMARY_ID;
      final byte extraDeviceId = missingDeviceId + 1;
      final byte staleDeviceId = extraDeviceId + 1;

      final Account destinationAccount = mock(Account.class);
      when(destinationAccount.getUnidentifiedAccessKey()).thenReturn(Optional.of(UNIDENTIFIED_ACCESS_KEY));

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
      when(accountsManager.getByServiceIdentifier(serviceIdentifier)).thenReturn(Optional.of(destinationAccount));

      final Map<Byte, IndividualRecipientMessageBundle.Message> messages = Map.of(
          staleDeviceId, IndividualRecipientMessageBundle.Message.newBuilder()
              .setRegistrationId(Device.PRIMARY_ID)
              .setPayload(ByteString.copyFrom(TestRandomUtil.nextBytes(128)))
              .build());

      doThrow(new MessageTooLargeException()).when(messageSender).sendMessages(any(), any(), any(), any(), any(), any());

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusInvalidArgument(
          () -> unauthenticatedServiceStub().sendStory(generateRequest(serviceIdentifier, false, messages)));
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
          () -> unauthenticatedServiceStub().sendStory(generateRequest(serviceIdentifier, true, messages)));

      verify(spamChecker).checkForIndividualRecipientSpamGrpc(MessageType.INDIVIDUAL_STORY,
          Optional.empty(),
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

      assertEquals(response, unauthenticatedServiceStub().sendStory(
          generateRequest(serviceIdentifier, true, messages)));

      verify(spamChecker).checkForIndividualRecipientSpamGrpc(MessageType.INDIVIDUAL_STORY,
          Optional.empty(),
          Optional.of(destinationAccount),
          serviceIdentifier);

      verify(messageSender, never()).sendMessages(any(), any(), any(), any(), any(), any());
    }

    private static SendStoryMessageRequest generateRequest(final ServiceIdentifier serviceIdentifier,
        final boolean urgent,
        final Map<Byte, IndividualRecipientMessageBundle.Message> messages) {

      final IndividualRecipientMessageBundle.Builder messageBundleBuilder = IndividualRecipientMessageBundle.newBuilder()
          .setTimestamp(CLOCK.millis());

      messages.forEach(messageBundleBuilder::putMessages);

      return SendStoryMessageRequest.newBuilder()
          .setDestination(ServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier))
          .setMessages(messageBundleBuilder)
          .setUrgent(urgent)
          .build();
    }
  }

  @Nested
  class MultiRecipientStory {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void sendStory(final boolean urgent) throws MessageTooLargeException, MultiRecipientMismatchedDevicesException {
      final byte deviceId = Device.PRIMARY_ID;
      final int registrationId = 7;

      final Device destinationDevice = DevicesHelper.createDevice(deviceId, CLOCK.millis(), registrationId);

      final Account resolvedAccount = mock(Account.class);
      when(resolvedAccount.getDevices()).thenReturn(List.of(destinationDevice));
      when(resolvedAccount.getDevice(deviceId)).thenReturn(Optional.of(destinationDevice));

      final AciServiceIdentifier resolvedServiceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
      final AciServiceIdentifier unresolvedServiceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

      when(accountsManager.getByServiceIdentifierAsync(resolvedServiceIdentifier))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(resolvedAccount)));

      final TestRecipient resolvedRecipient =
          new TestRecipient(resolvedServiceIdentifier, deviceId, registrationId, new byte[48]);

      final TestRecipient unresolvedRecipient =
          new TestRecipient(unresolvedServiceIdentifier, Device.PRIMARY_ID, 1, new byte[48]);

      final byte[] payload = MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(
          resolvedRecipient, unresolvedRecipient));

      final SendMultiRecipientStoryRequest request = SendMultiRecipientStoryRequest.newBuilder()
          .setMessage(MultiRecipientMessage.newBuilder()
              .setTimestamp(CLOCK.millis())
              .setPayload(ByteString.copyFrom(payload))
              .build())
          .setUrgent(urgent)
          .build();

      assertEquals(SendMultiRecipientMessageResponse.newBuilder().build(),
          unauthenticatedServiceStub().sendMultiRecipientStory(request));

      verify(messageSender).sendMultiRecipientMessage(any(),
          argThat(resolvedRecipients -> resolvedRecipients.containsValue(resolvedAccount)),
          eq(CLOCK.millis()),
          eq(true),
          eq(false),
          eq(urgent),
          any());
    }

    @Test
    void mismatchedDevices() throws MessageTooLargeException, MultiRecipientMismatchedDevicesException {
      final byte missingDeviceId = Device.PRIMARY_ID;
      final byte extraDeviceId = missingDeviceId + 1;
      final byte staleDeviceId = extraDeviceId + 1;

      final Account destinationAccount = mock(Account.class);

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

      when(accountsManager.getByServiceIdentifierAsync(serviceIdentifier))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(destinationAccount)));

      final byte[] payload = MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(
          new TestRecipient(serviceIdentifier, staleDeviceId, 17, new byte[48])));

      final SendMultiRecipientStoryRequest request = SendMultiRecipientStoryRequest.newBuilder()
          .setMessage(MultiRecipientMessage.newBuilder()
              .setTimestamp(CLOCK.millis())
              .setPayload(ByteString.copyFrom(payload))
              .build())
          .setUrgent(true)
          .build();

      doThrow(new MultiRecipientMismatchedDevicesException(Map.of(serviceIdentifier,
          new org.whispersystems.textsecuregcm.controllers.MismatchedDevices(
              Set.of(missingDeviceId), Set.of(extraDeviceId), Set.of(staleDeviceId)))))
          .when(messageSender).sendMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean(), any());

      final SendMultiRecipientMessageResponse response =
          unauthenticatedServiceStub().sendMultiRecipientStory(request);

      final SendMultiRecipientMessageResponse expectedResponse = SendMultiRecipientMessageResponse.newBuilder()
          .setMismatchedDevices(MultiRecipientMismatchedDevices.newBuilder()
              .addMismatchedDevices(MismatchedDevices.newBuilder()
                  .setServiceIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier))
                  .addMissingDevices(missingDeviceId)
                  .addExtraDevices(extraDeviceId)
                  .addStaleDevices(staleDeviceId)
                  .build())
              .build())
          .build();

      assertEquals(expectedResponse, response);
    }

    @Test
    void badPayload() throws MessageTooLargeException, MultiRecipientMismatchedDevicesException {
      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT, () ->
          unauthenticatedServiceStub().sendMultiRecipientStory(SendMultiRecipientStoryRequest.newBuilder()
              .setMessage(MultiRecipientMessage.newBuilder()
                  .setTimestamp(CLOCK.millis())
                  .setPayload(ByteString.copyFrom(TestRandomUtil.nextBytes(128)))
                  .build())
              .build()));

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT, () ->
          unauthenticatedServiceStub().sendMultiRecipientMessage(
              SendMultiRecipientMessageRequest.newBuilder().build()));

      verify(messageSender, never())
          .sendMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean(), any());
    }

    @Test
    void repeatedRecipient() throws MessageTooLargeException, MultiRecipientMismatchedDevicesException {
      final Device destinationDevice = DevicesHelper.createDevice(Device.PRIMARY_ID, CLOCK.millis(), 1);

      final Account destinationAccount = mock(Account.class);
      when(destinationAccount.getDevices()).thenReturn(List.of(destinationDevice));
      when(destinationAccount.getDevice(Device.PRIMARY_ID)).thenReturn(Optional.of(destinationDevice));

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

      when(accountsManager.getByServiceIdentifierAsync(serviceIdentifier))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(destinationAccount)));

      final TestRecipient recipient = new TestRecipient(serviceIdentifier, Device.PRIMARY_ID, 1, new byte[48]);

      final byte[] payload = MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(recipient, recipient));

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT, () ->
          unauthenticatedServiceStub().sendMultiRecipientStory(SendMultiRecipientStoryRequest.newBuilder()
              .setMessage(MultiRecipientMessage.newBuilder()
                  .setTimestamp(CLOCK.millis())
                  .setPayload(ByteString.copyFrom(payload))
                  .build())
              .setUrgent(true)
              .build()));

      verify(messageSender, never())
          .sendMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean(), any());
    }

    @Test
    void oversizedMessage() throws MessageTooLargeException, MultiRecipientMismatchedDevicesException {
      final Account destinationAccount = mock(Account.class);

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

      when(accountsManager.getByServiceIdentifierAsync(serviceIdentifier))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(destinationAccount)));

      final byte[] payload = MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(
          new TestRecipient(serviceIdentifier, Device.PRIMARY_ID, 17, new byte[48])));

      final SendMultiRecipientStoryRequest request = SendMultiRecipientStoryRequest.newBuilder()
          .setMessage(MultiRecipientMessage.newBuilder()
              .setTimestamp(CLOCK.millis())
              .setPayload(ByteString.copyFrom(payload))
              .build())
          .setUrgent(true)
          .build();

      doThrow(new MessageTooLargeException())
          .when(messageSender).sendMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean(), any());

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusInvalidArgument(() -> unauthenticatedServiceStub().sendMultiRecipientStory(request));
    }

    @Test
    void spamWithStatus() throws MessageTooLargeException, MultiRecipientMismatchedDevicesException {
      final byte deviceId = Device.PRIMARY_ID;
      final int registrationId = 7;

      final Device destinationDevice = DevicesHelper.createDevice(deviceId, CLOCK.millis(), registrationId);

      final Account destinationAccount = mock(Account.class);
      when(destinationAccount.getDevices()).thenReturn(List.of(destinationDevice));
      when(destinationAccount.getDevice(deviceId)).thenReturn(Optional.of(destinationDevice));

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

      when(accountsManager.getByServiceIdentifierAsync(serviceIdentifier))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(destinationAccount)));

      final TestRecipient recipient =
          new TestRecipient(serviceIdentifier, deviceId, registrationId, new byte[48]);

      final byte[] payload = MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(recipient));

      final SendMultiRecipientStoryRequest request = SendMultiRecipientStoryRequest.newBuilder()
          .setMessage(MultiRecipientMessage.newBuilder()
              .setTimestamp(CLOCK.millis())
              .setPayload(ByteString.copyFrom(payload))
              .build())
          .setUrgent(true)
          .build();

      when(spamChecker.checkForMultiRecipientSpamGrpc(any()))
          .thenReturn(new SpamCheckResult<>(Optional.of(GrpcResponse.withStatus(Status.RESOURCE_EXHAUSTED)), Optional.empty()));

      //noinspection ResultOfMethodCallIgnored
      GrpcTestUtils.assertStatusException(Status.RESOURCE_EXHAUSTED,
          () -> unauthenticatedServiceStub().sendMultiRecipientStory(request));

      verify(spamChecker).checkForMultiRecipientSpamGrpc(MessageType.MULTI_RECIPIENT_STORY);

      verify(messageSender, never())
          .sendMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean(), any());
    }

    @Test
    void spamWithResponse() throws MessageTooLargeException, MultiRecipientMismatchedDevicesException {
      final byte deviceId = Device.PRIMARY_ID;
      final int registrationId = 7;

      final Device destinationDevice = DevicesHelper.createDevice(deviceId, CLOCK.millis(), registrationId);

      final Account destinationAccount = mock(Account.class);
      when(destinationAccount.getDevices()).thenReturn(List.of(destinationDevice));
      when(destinationAccount.getDevice(deviceId)).thenReturn(Optional.of(destinationDevice));

      final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

      when(accountsManager.getByServiceIdentifierAsync(serviceIdentifier))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(destinationAccount)));

      final TestRecipient recipient =
          new TestRecipient(serviceIdentifier, deviceId, registrationId, new byte[48]);

      final byte[] payload = MultiRecipientMessageHelper.generateMultiRecipientMessage(List.of(recipient));

      final SendMultiRecipientStoryRequest request = SendMultiRecipientStoryRequest.newBuilder()
          .setMessage(MultiRecipientMessage.newBuilder()
              .setTimestamp(CLOCK.millis())
              .setPayload(ByteString.copyFrom(payload))
              .build())
          .setUrgent(true)
          .build();

      final SendMultiRecipientMessageResponse response = SendMultiRecipientMessageResponse.newBuilder()
          .setChallengeRequired(ChallengeRequired.newBuilder()
              .addChallengeOptions(ChallengeRequired.ChallengeType.CAPTCHA))
          .build();

      when(spamChecker.checkForMultiRecipientSpamGrpc(any()))
          .thenReturn(new SpamCheckResult<>(Optional.of(GrpcResponse.withResponse(response)), Optional.empty()));

      assertEquals(response, unauthenticatedServiceStub().sendMultiRecipientStory(request));

      verify(spamChecker).checkForMultiRecipientSpamGrpc(MessageType.MULTI_RECIPIENT_STORY);

      verify(messageSender, never())
          .sendMultiRecipientMessage(any(), any(), anyLong(), anyBoolean(), anyBoolean(), anyBoolean(), any());
    }
  }
}
