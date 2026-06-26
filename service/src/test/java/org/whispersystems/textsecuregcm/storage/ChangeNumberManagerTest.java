/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.protobuf.ByteString;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.stubbing.Answer;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.PhoneVerificationTokenManager;
import org.whispersystems.textsecuregcm.auth.RegistrationLockVerificationManager;
import org.whispersystems.textsecuregcm.controllers.MessageDeliveryNotAllowedException;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.IncomingMessage;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.PhoneVerificationRequest;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.MessageTooLargeException;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class ChangeNumberManagerTest {
  private MessageSender messageSender;
  private AccountsManager accountsManager;
  private PhoneVerificationTokenManager phoneVerificationTokenManager;
  private RegistrationLockVerificationManager registrationLockVerificationManager;
  private ChangeNumberWaitingPeriodManager changeNumberWaitingPeriodManager;
  private RateLimiter rateLimiter;

  private ChangeNumberManager changeNumberManager;

  private Map<Account, UUID> updatedPhoneNumberIdentifiersByAccount;

  private static final TestClock CLOCK = TestClock.pinned(Instant.now());

  @BeforeEach
  void setUp() throws Exception {
    messageSender = mock(MessageSender.class);
    accountsManager = mock(AccountsManager.class);
    registrationLockVerificationManager = mock(RegistrationLockVerificationManager.class);
    phoneVerificationTokenManager = mock(PhoneVerificationTokenManager.class);
    changeNumberWaitingPeriodManager = mock(ChangeNumberWaitingPeriodManager.class);
    rateLimiter = mock(RateLimiter.class);

    when(phoneVerificationTokenManager.verify(any(), any(), any(), any())).thenAnswer(invocation -> {
      final byte[] sessionId = invocation.getArgument(2);

      return sessionId != null
          ? PhoneVerificationRequest.VerificationType.SESSION
          : PhoneVerificationRequest.VerificationType.RECOVERY_PASSWORD;
    });

    when(changeNumberWaitingPeriodManager.getWaitingPeriodRemaining(any())).thenReturn(Optional.empty());

    final RateLimiters rateLimiters = mock(RateLimiters.class);
    when(rateLimiters.getRegistrationLimiter()).thenReturn(rateLimiter);

    changeNumberManager = new ChangeNumberManager(messageSender, accountsManager,
        phoneVerificationTokenManager, registrationLockVerificationManager, rateLimiters,
        changeNumberWaitingPeriodManager, CLOCK);

    updatedPhoneNumberIdentifiersByAccount = new HashMap<>();

    when(accountsManager.changeNumber(any(), any(), any(), any(), any(), any())).thenAnswer((Answer<Account>)invocation -> {
      final UUID accountIdentifier = invocation.getArgument(0);
      final String number = invocation.getArgument(1);

      final Account account =
          accountsManager.getAccountsForChangeNumber(accountIdentifier, number).first();

      final UUID uuid = account.getIdentifier(IdentityType.ACI);
      final List<Device> devices = account.getDevices();
      final Device primaryDevice = account.getPrimaryDevice();

      final UUID updatedPni = UUID.randomUUID();
      updatedPhoneNumberIdentifiersByAccount.put(account, updatedPni);

      final Account updatedAccount = mock(Account.class);
      when(updatedAccount.getIdentifier(IdentityType.ACI)).thenReturn(uuid);
      when(updatedAccount.getIdentifier(IdentityType.PNI)).thenReturn(updatedPni);
      when(updatedAccount.isIdentifiedBy(any())).thenReturn(false);
      when(updatedAccount.isIdentifiedBy(new AciServiceIdentifier(uuid))).thenReturn(true);
      when(updatedAccount.isIdentifiedBy(new PniServiceIdentifier(updatedPni))).thenReturn(true);
      when(updatedAccount.getNumber()).thenReturn(number);
      when(updatedAccount.getDevices()).thenReturn(devices);
      when(updatedAccount.getDevice(anyByte())).thenReturn(Optional.empty());
      when(updatedAccount.getPrimaryDevice()).thenReturn(primaryDevice);

      account.getDevices().forEach(device ->
          when(updatedAccount.getDevice(device.getId())).thenReturn(Optional.of(device)));

      return updatedAccount;
    });
  }

  @Test
  void changeNumberSingleDevice() throws Exception {
    final String originalNumber = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("DE"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final String targetNumber = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey pniIdentityKey = new IdentityKey(ECKeyPair.generate().getPublicKey());

    final Map<Byte, ECSignedPreKey> ecSignedPreKeys =
        Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, pniIdentityKeyPair));

    final Map<Byte, KEMSignedPreKey> kemLastResortPreKeys =
        Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, pniIdentityKeyPair));

    final UUID accountIdentifier = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn(originalNumber);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(account.isIdentifiedBy(any())).thenReturn(false);
    when(account.isIdentifiedBy(new AciServiceIdentifier(accountIdentifier))).thenReturn(true);

    when(accountsManager.getAccountsForChangeNumber(eq(accountIdentifier), any()))
        .thenReturn(new Pair<>(account, Optional.empty()));

    changeNumberManager.changeNumber(accountIdentifier, null, null, null, targetNumber, pniIdentityKey, ecSignedPreKeys, kemLastResortPreKeys, Collections.emptyList(), Collections.emptyMap(), mock(ContainerRequestContext.class));
    verify(accountsManager).changeNumber(accountIdentifier, targetNumber, pniIdentityKey, ecSignedPreKeys, kemLastResortPreKeys, Collections.emptyMap());
    verify(messageSender, never()).sendMessages(any(), any(), any(), any(), any(), any());
  }

  @Test
  void changeNumberLinkedDevices() throws Exception {
    final String originalNumber = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("DE"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final String targetNumber = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final UUID aci = UUID.randomUUID();

    final byte primaryDeviceId = Device.PRIMARY_ID;
    final byte linkedDeviceId = primaryDeviceId + 1;

    final int primaryDeviceRegistrationId = 17;
    final int linkedDeviceRegistrationId = primaryDeviceRegistrationId + 1;

    final Device primaryDevice = mock(Device.class);
    when(primaryDevice.getId()).thenReturn(primaryDeviceId);
    when(primaryDevice.getRegistrationId(IdentityType.ACI)).thenReturn(primaryDeviceRegistrationId);

    final Device linkedDevice = mock(Device.class);
    when(linkedDevice.getId()).thenReturn(linkedDeviceId);
    when(linkedDevice.getRegistrationId(IdentityType.ACI)).thenReturn(linkedDeviceRegistrationId);

    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn(originalNumber);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(aci);
    when(account.isIdentifiedBy(any())).thenReturn(false);
    when(account.isIdentifiedBy(new AciServiceIdentifier(aci))).thenReturn(true);
    when(account.getDevice(anyByte())).thenReturn(Optional.empty());
    when(account.getDevice(primaryDeviceId)).thenReturn(Optional.of(primaryDevice));
    when(account.getDevice(linkedDeviceId)).thenReturn(Optional.of(linkedDevice));
    when(account.getDevices()).thenReturn(List.of(primaryDevice, linkedDevice));

    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());
    final Map<Byte, ECSignedPreKey> ecSignedPreKeys = Map.of(
        primaryDeviceId, KeysHelper.signedECPreKey(1, pniIdentityKeyPair),
        linkedDeviceId, KeysHelper.signedECPreKey(2, pniIdentityKeyPair));

    final Map<Byte, KEMSignedPreKey> kemLastResortPreKeys = Map.of(
        primaryDeviceId, KeysHelper.signedKEMPreKey(3, pniIdentityKeyPair),
        linkedDeviceId, KeysHelper.signedKEMPreKey(4, pniIdentityKeyPair));

    final Map<Byte, Integer> registrationIds = Map.of(
        primaryDeviceId, primaryDeviceRegistrationId,
        linkedDeviceId, linkedDeviceRegistrationId);

    final IncomingMessage incomingMessage =
        new IncomingMessage(1, linkedDeviceId, linkedDeviceRegistrationId, new byte[] { 1 });

    when(accountsManager.getAccountsForChangeNumber(eq(aci), any()))
        .thenReturn(new Pair<>(account, Optional.empty()));

    changeNumberManager.changeNumber(aci,
        null,
        null,
        null,
        targetNumber,
        pniIdentityKey,
        ecSignedPreKeys,
        kemLastResortPreKeys,
        List.of(incomingMessage),
        registrationIds,
        mock(ContainerRequestContext.class));

    verify(accountsManager).changeNumber(aci,
        targetNumber,
        pniIdentityKey,
        ecSignedPreKeys,
        kemLastResortPreKeys,
        registrationIds);

    final MessageProtos.Envelope expectedEnvelope = MessageProtos.Envelope.newBuilder()
        .setType(MessageProtos.Envelope.Type.forNumber(incomingMessage.type()))
        .setClientTimestamp(CLOCK.millis())
        .setServerTimestamp(CLOCK.millis())
        .setDestinationServiceId(new AciServiceIdentifier(aci).toCompactByteString())
        .setContent(ByteString.copyFrom(incomingMessage.content()))
        .setSourceServiceId(new AciServiceIdentifier(aci).toCompactByteString())
        .setSourceDevice(primaryDeviceId)
        .setUpdatedPni(UUIDUtil.toByteString(updatedPhoneNumberIdentifiersByAccount.get(account)))
        .setUrgent(true)
        .setEphemeral(false)
        .build();

    verify(messageSender).sendMessages(argThat(a -> a.getIdentifier(IdentityType.ACI).equals(aci)),
        eq(new AciServiceIdentifier(aci)),
        eq(Map.of(linkedDeviceId, expectedEnvelope)),
        eq(Map.of(linkedDeviceId, linkedDeviceRegistrationId)),
        eq(Optional.of(primaryDeviceId)),
        any());
  }

  @Test
  void changeNumberSameNumber() throws Exception {
    final String targetNumber = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey pniIdentityKey = new IdentityKey(ECKeyPair.generate().getPublicKey());

    final Map<Byte, ECSignedPreKey> ecSignedPreKeys =
        Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, pniIdentityKeyPair));

    final Map<Byte, KEMSignedPreKey> kemLastResortPreKeys =
        Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, pniIdentityKeyPair));

    final UUID accountIdentifier = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn(targetNumber);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(account.isIdentifiedBy(any())).thenReturn(false);
    when(account.isIdentifiedBy(new AciServiceIdentifier(accountIdentifier))).thenReturn(true);

    when(accountsManager.getAccountsForChangeNumber(eq(accountIdentifier), any()))
        .thenReturn(new Pair<>(account, Optional.empty()));

    changeNumberManager.changeNumber(accountIdentifier, null, null, null, targetNumber, pniIdentityKey, ecSignedPreKeys, kemLastResortPreKeys, Collections.emptyList(), Collections.emptyMap(), mock(ContainerRequestContext.class));

    verifyNoInteractions(rateLimiter);
    verifyNoInteractions(phoneVerificationTokenManager);
    verifyNoInteractions(registrationLockVerificationManager);

    verify(accountsManager).changeNumber(accountIdentifier, targetNumber, pniIdentityKey, ecSignedPreKeys, kemLastResortPreKeys, Collections.emptyMap());
    verify(messageSender, never()).sendMessages(eq(account), any(), any(), any(), any(), any());
  }

  @Test
  void changeNumberRateLimited()
      throws MismatchedDevicesException, InterruptedException, MessageTooLargeException, RateLimitExceededException, MessageDeliveryNotAllowedException {
    final String originalNumber = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("DE"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final String targetNumber = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey pniIdentityKey = new IdentityKey(ECKeyPair.generate().getPublicKey());

    final Map<Byte, ECSignedPreKey> ecSignedPreKeys =
        Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, pniIdentityKeyPair));

    final Map<Byte, KEMSignedPreKey> kemLastResortPreKeys =
        Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, pniIdentityKeyPair));

    final UUID accountIdentifier = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn(originalNumber);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(account.isIdentifiedBy(any())).thenReturn(false);
    when(account.isIdentifiedBy(new AciServiceIdentifier(accountIdentifier))).thenReturn(true);

    when(accountsManager.getAccountsForChangeNumber(eq(accountIdentifier), any()))
        .thenReturn(new Pair<>(account, Optional.empty()));

    doThrow(new RateLimitExceededException(Duration.ofMinutes(1)))
        .when(rateLimiter).validate(targetNumber);

    assertThrows(RateLimitExceededException.class, () -> changeNumberManager.changeNumber(accountIdentifier,
        null,
        null,
        null,
        targetNumber,
        pniIdentityKey,
        ecSignedPreKeys,
        kemLastResortPreKeys,
        Collections.emptyList(),
        Collections.emptyMap(),
        mock(ContainerRequestContext.class)));

    verify(accountsManager, never()).changeNumber(any(), any(), any(), any(), any(), any());
    verify(messageSender, never()).sendMessages(any(), any(), any(), any(), any(), any());
  }

  @Test
  void changeNumberRegistrationLockFailed()
      throws MismatchedDevicesException, InterruptedException, MessageTooLargeException, RateLimitExceededException, MessageDeliveryNotAllowedException {
    final String originalNumber = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("DE"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final String targetNumber = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey pniIdentityKey = new IdentityKey(ECKeyPair.generate().getPublicKey());

    final Map<Byte, ECSignedPreKey> ecSignedPreKeys =
        Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, pniIdentityKeyPair));

    final Map<Byte, KEMSignedPreKey> kemLastResortPreKeys =
        Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, pniIdentityKeyPair));

    final UUID accountIdentifier = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn(originalNumber);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(account.isIdentifiedBy(any())).thenReturn(false);
    when(account.isIdentifiedBy(new AciServiceIdentifier(accountIdentifier))).thenReturn(true);

    final Account existingAccount = mock(Account.class);
    when(existingAccount.getNumber()).thenReturn(targetNumber);

    when(accountsManager.getAccountsForChangeNumber(eq(accountIdentifier), any()))
        .thenReturn(new Pair<>(account, Optional.of(existingAccount)));

    doThrow(new WebApplicationException(423))
        .when(registrationLockVerificationManager).verifyRegistrationLock(eq(existingAccount), any(), any(), any(), any());

    assertThrows(WebApplicationException.class, () -> changeNumberManager.changeNumber(accountIdentifier,
        null,
        null,
        null,
        targetNumber,
        pniIdentityKey,
        ecSignedPreKeys,
        kemLastResortPreKeys,
        Collections.emptyList(),
        Collections.emptyMap(),
        mock(ContainerRequestContext.class)));

    verify(accountsManager, never()).changeNumber(any(), any(), any(), any(), any(), any());
    verify(messageSender, never()).sendMessages(any(), any(), any(), any(), any(), any());
  }

  @ParameterizedTest
  @MethodSource
  void testRecentRegistration(final boolean expectRateLimited, final boolean sameNumber, final boolean waitingPeriodMet) throws Throwable {

    final Duration waitingPeriod = Duration.ofMinutes(30);

    reset(changeNumberWaitingPeriodManager);
    when(changeNumberWaitingPeriodManager.getWaitingPeriodRemaining(any()))
        .thenReturn(waitingPeriodMet ? Optional.empty() : Optional.of(waitingPeriod));

    final String originalNumber = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("DE"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final String targetNumber = sameNumber
        ? originalNumber
        : PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey pniIdentityKey = new IdentityKey(ECKeyPair.generate().getPublicKey());

    final Map<Byte, ECSignedPreKey> ecSignedPreKeys =
        Map.of(Device.PRIMARY_ID, KeysHelper.signedECPreKey(1, pniIdentityKeyPair));

    final Map<Byte, KEMSignedPreKey> kemLastResortPreKeys =
        Map.of(Device.PRIMARY_ID, KeysHelper.signedKEMPreKey(2, pniIdentityKeyPair));

    final UUID accountIdentifier = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getNumber()).thenReturn(originalNumber);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(account.isIdentifiedBy(any())).thenReturn(false);
    when(account.isIdentifiedBy(new AciServiceIdentifier(accountIdentifier))).thenReturn(true);

    when(accountsManager.getAccountsForChangeNumber(eq(accountIdentifier), any()))
        .thenReturn(new Pair<>(account, Optional.empty()));

    final Executable changeNumberOperation = () -> changeNumberManager.changeNumber(accountIdentifier,
        null,
        null,
        null,
        targetNumber,
        pniIdentityKey,
        ecSignedPreKeys,
        kemLastResortPreKeys,
        Collections.emptyList(),
        Collections.emptyMap(),
        mock(ContainerRequestContext.class));
    if (expectRateLimited) {
      final RateLimitExceededException e = assertThrows(RateLimitExceededException.class, changeNumberOperation);
      assertEquals(waitingPeriod, e.getRetryDuration().orElseThrow());
    } else {
      changeNumberOperation.execute();
      verify(accountsManager).changeNumber(accountIdentifier, targetNumber, pniIdentityKey, ecSignedPreKeys, kemLastResortPreKeys, Collections.emptyMap());
    }
  }

  static Collection<Arguments> testRecentRegistration() {
    return List.of(
        // expect exception, same number, registration instant
        Arguments.argumentSet("waiting period elapsed", false, false, true),
        Arguments.argumentSet("waiting period not elapsed", true, false, false),
        Arguments.argumentSet("waiting period not elapsed; same number", false, true, false)
    );
  }
}
