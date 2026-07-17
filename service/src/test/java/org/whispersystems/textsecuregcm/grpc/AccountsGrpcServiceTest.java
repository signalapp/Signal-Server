/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.Status;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.signal.chat.account.AccountsGrpc;
import org.signal.chat.account.Capabilities;
import org.signal.chat.account.ChangeNumberRequest;
import org.signal.chat.account.ChangeNumberResponse;
import org.signal.chat.account.ClearRegistrationLockRequest;
import org.signal.chat.account.ClearRegistrationLockResponse;
import org.signal.chat.account.ConfigureUnidentifiedAccessRequest;
import org.signal.chat.account.ConfirmUsernameHashRequest;
import org.signal.chat.account.ConfirmUsernameHashResponse;
import org.signal.chat.account.DeleteAccountRequest;
import org.signal.chat.account.DeleteAccountResponse;
import org.signal.chat.account.DeleteUsernameHashRequest;
import org.signal.chat.account.DeleteUsernameLinkRequest;
import org.signal.chat.account.ExternalServiceCredentials;
import org.signal.chat.account.GetAccountDataReportRequest;
import org.signal.chat.account.GetAccountDataReportResponse;
import org.signal.chat.account.GetAccountIdentityRequest;
import org.signal.chat.account.GetAccountIdentityResponse;
import org.signal.chat.account.GetCapabilitiesRequest;
import org.signal.chat.account.GetCapabilitiesResponse;
import org.signal.chat.account.GetEntitlementsRequest;
import org.signal.chat.account.GetEntitlementsResponse;
import org.signal.chat.account.RegistrationLockFailure;
import org.signal.chat.account.ReserveUsernameHashRequest;
import org.signal.chat.account.ReserveUsernameHashResponse;
import org.signal.chat.account.SetDiscoverableByPhoneNumberRequest;
import org.signal.chat.account.SetRegistrationLockRequest;
import org.signal.chat.account.SetRegistrationLockResponse;
import org.signal.chat.account.SetRegistrationRecoveryPasswordRequest;
import org.signal.chat.account.SetUsernameLinkRequest;
import org.signal.chat.account.SetUsernameLinkResponse;
import org.signal.chat.account.SetZkCredentialKeyRequest;
import org.signal.chat.account.SetZkCredentialKeyResponse;
import org.signal.chat.account.StaleDevices;
import org.signal.chat.account.UsernameNotAvailable;
import org.signal.chat.common.AccountIdentifiers;
import org.signal.chat.common.EcSignedPreKey;
import org.signal.chat.common.KemSignedPreKey;
import org.signal.chat.errors.FailedPrecondition;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.usernames.BaseUsernameException;
import org.signal.libsignal.zkgroup.ZkCredentialKeyPair;
import org.signal.libsignal.zkgroup.ZkCredentialPublicKey;
import org.whispersystems.textsecuregcm.auth.InvalidRegistrationSessionException;
import org.whispersystems.textsecuregcm.auth.RecoveryPasswordVerificationFailedException;
import org.whispersystems.textsecuregcm.auth.RegistrationLockFailureException;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.auth.UnverifiedRegistrationSessionException;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.MessageDeliveryNotAllowedException;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevices;
import org.whispersystems.textsecuregcm.controllers.MismatchedDevicesException;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.entities.EncryptedUsername;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.MessageTooLargeException;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ChangeNumberManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.storage.KeyIdUtil;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.UsernameHashNotAvailableException;
import org.whispersystems.textsecuregcm.storage.UsernameReservationNotFoundException;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AccountsTestHelper;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.UsernameHashZkProofVerifier;

class AccountsGrpcServiceTest extends SimpleBaseGrpcTest<AccountsGrpcService, AccountsGrpc.AccountsBlockingStub> {

  @Mock
  private AccountsManager accountsManager;

  @Mock
  private RateLimiter rateLimiter;

  @Mock
  private UsernameHashZkProofVerifier usernameHashZkProofVerifier;

  @Mock
  private RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;

  private final TestClock testClock = TestClock.pinned(Instant.now());

  @Mock
  private ChangeNumberManager changeNumberManager;

  @Override
  protected AccountsGrpcService createServiceBeforeEachTest() {
    AccountsHelper.setupMockUpdate(accountsManager);

    final RateLimiters rateLimiters = mock(RateLimiters.class);
    when(rateLimiters.getUsernameReserveLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getUsernameSetLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getUsernameLinkOperationLimiter()).thenReturn(rateLimiter);

    when(rateLimiters.getSetZkCredentialKeyLimiter()).thenReturn(rateLimiter);

    return new AccountsGrpcService(accountsManager,
        rateLimiters,
        usernameHashZkProofVerifier,
        registrationRecoveryPasswordsManager,
        testClock,
        changeNumberManager);
  }

  @Test
  void getAuthenticatedAccountIdentity() {
    final UUID phoneNumberIdentifier = UUID.randomUUID();
    final String e164 = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final byte[] usernameHash = TestRandomUtil.nextBytes(32);
    final UUID usernameLinkHandle = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(AUTHENTICATED_ACI);
    when(account.getPhoneNumberIdentifier()).thenReturn(phoneNumberIdentifier);
    when(account.getNumber()).thenReturn(e164);
    when(account.getUsernameHash()).thenReturn(Optional.of(usernameHash));
    when(account.getUsernameLinkHandle()).thenReturn(usernameLinkHandle);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));

    final GetAccountIdentityResponse expectedResponse = GetAccountIdentityResponse.newBuilder()
        .setAccountIdentifiers(AccountIdentifiers.newBuilder()
            .addServiceIdentifiers(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(AUTHENTICATED_ACI)))
            .addServiceIdentifiers(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(new PniServiceIdentifier(phoneNumberIdentifier)))
            .setE164(e164)
            .setUsernameHash(ByteString.copyFrom(usernameHash))
            .setUsernameLinkHandle(UUIDUtil.toByteString(usernameLinkHandle))
            .build())
        .build();

    assertEquals(expectedResponse, authenticatedServiceStub().getAccountIdentity(GetAccountIdentityRequest.newBuilder().build()));
  }


  @Test
  void deleteAccount() {
    final DeleteAccountResponse ignored =
        authenticatedServiceStub().deleteAccount(DeleteAccountRequest.newBuilder().build());

    verify(accountsManager).delete(AUTHENTICATED_ACI, AccountsManager.DeletionReason.USER_REQUEST);
  }

  @Test
  void deleteAccountLinkedDevice() {
    getMockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, (byte) (Device.PRIMARY_ID + 1));

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT, "BAD_AUTHENTICATION",
        () -> authenticatedServiceStub().deleteAccount(DeleteAccountRequest.newBuilder().build()));

    verify(accountsManager, never()).delete(any(), any());
  }

  @Test
  void setRegistrationLock() {
    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));

    final byte[] registrationLockSecret = TestRandomUtil.nextBytes(32);

    final SetRegistrationLockResponse ignored =
        authenticatedServiceStub().setRegistrationLock(SetRegistrationLockRequest.newBuilder()
            .setRegistrationLock(ByteString.copyFrom(registrationLockSecret))
            .build());

    final ArgumentCaptor<String> hashCaptor = ArgumentCaptor.forClass(String.class);
    final ArgumentCaptor<String> saltCaptor = ArgumentCaptor.forClass(String.class);

    verify(account).setRegistrationLock(hashCaptor.capture(), saltCaptor.capture());

    final SaltedTokenHash registrationLock = new SaltedTokenHash(hashCaptor.getValue(), saltCaptor.getValue());
    assertTrue(registrationLock.verify(HexFormat.of().formatHex(registrationLockSecret)));
  }

  @Test
  void setRegistrationLockEmptySecret() {
    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> authenticatedServiceStub().setRegistrationLock(SetRegistrationLockRequest.newBuilder()
            .build()));

    verify(accountsManager, never()).update(any(UUID.class), any());
  }

  @Test
  void setRegistrationLockLinkedDevice() {
    getMockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, (byte) (Device.PRIMARY_ID + 1));

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT, "BAD_AUTHENTICATION",
        () -> authenticatedServiceStub().setRegistrationLock(SetRegistrationLockRequest.newBuilder()
                .setRegistrationLock(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .build()));

    verify(accountsManager, never()).update(any(UUID.class), any());
  }

  @Test
  void clearRegistrationLock() {
    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));

    final ClearRegistrationLockResponse ignored =
        authenticatedServiceStub().clearRegistrationLock(ClearRegistrationLockRequest.newBuilder().build());

    verify(account).setRegistrationLock(null, null);
  }

  @Test
  void clearRegistrationLockLinkedDevice() {
    getMockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, (byte) (Device.PRIMARY_ID + 1));

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> authenticatedServiceStub().clearRegistrationLock(ClearRegistrationLockRequest.newBuilder().build()));

    verify(accountsManager, never()).update(any(UUID.class), any());
  }

  @Test
  void reserveUsernameHash() throws UsernameHashNotAvailableException {
    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(AUTHENTICATED_ACI);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));

    final byte[] usernameHash = TestRandomUtil.nextBytes(AccountController.USERNAME_HASH_LENGTH);

    when(accountsManager.reserveUsernameHash(any(), any()))
        .thenAnswer(invocation -> {
          final List<byte[]> usernameHashes = invocation.getArgument(1);

          return new AccountsManager.UsernameReservation(accountsManager.getByAccountIdentifier(invocation.getArgument(0)).orElseThrow(), usernameHashes.getFirst());
        });

    final ReserveUsernameHashResponse expectedResponse = ReserveUsernameHashResponse.newBuilder()
        .setUsernameHash(ByteString.copyFrom(usernameHash))
        .build();

    assertEquals(expectedResponse,
        authenticatedServiceStub().reserveUsernameHash(ReserveUsernameHashRequest.newBuilder()
            .addUsernameHashes(ByteString.copyFrom(usernameHash))
            .build()));
  }

  @Test
  void reserveUsernameHashNotAvailable() throws UsernameHashNotAvailableException {
    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));

    final byte[] usernameHash = TestRandomUtil.nextBytes(AccountController.USERNAME_HASH_LENGTH);

    when(accountsManager.reserveUsernameHash(any(), any()))
        .thenThrow(new UsernameHashNotAvailableException());

    final ReserveUsernameHashResponse expectedResponse = ReserveUsernameHashResponse.newBuilder()
        .setUsernameNotAvailable(UsernameNotAvailable.getDefaultInstance())
        .build();

    assertEquals(expectedResponse,
        authenticatedServiceStub().reserveUsernameHash(ReserveUsernameHashRequest.newBuilder()
            .addUsernameHashes(ByteString.copyFrom(usernameHash))
            .build()));
  }

  @Test
  void reserveUsernameHashNoHashes() {
    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> authenticatedServiceStub().reserveUsernameHash(ReserveUsernameHashRequest.newBuilder().build()));
  }

  @Test
  void reserveUsernameHashTooManyHashes() {
    final ReserveUsernameHashRequest.Builder requestBuilder = ReserveUsernameHashRequest.newBuilder();

    for (int i = 0; i < AccountController.MAXIMUM_USERNAME_HASHES_LIST_LENGTH + 1; i++) {
      final byte[] usernameHash = TestRandomUtil.nextBytes(AccountController.USERNAME_HASH_LENGTH);
      requestBuilder.addUsernameHashes(ByteString.copyFrom(usernameHash));
    }

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> authenticatedServiceStub().reserveUsernameHash(requestBuilder.build()));
  }

  @Test
  void reserveUsernameHashBadHashLength() {
    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));

    final byte[] usernameHash = TestRandomUtil.nextBytes(AccountController.USERNAME_HASH_LENGTH + 1);

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> authenticatedServiceStub().reserveUsernameHash(ReserveUsernameHashRequest.newBuilder()
            .addUsernameHashes(ByteString.copyFrom(usernameHash))
            .build()));
  }

  @Test
  void reserveUsernameHashRateLimited() throws RateLimitExceededException {
    final byte[] usernameHash = TestRandomUtil.nextBytes(AccountController.USERNAME_HASH_LENGTH);

    final Duration retryAfter = Duration.ofMinutes(3);

    doThrow(new RateLimitExceededException(retryAfter))
        .when(rateLimiter).validate(any(UUID.class));

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertRateLimitExceeded(retryAfter,
        () -> authenticatedServiceStub().reserveUsernameHash(ReserveUsernameHashRequest.newBuilder()
            .addUsernameHashes(ByteString.copyFrom(usernameHash))
            .build()),
        accountsManager);
  }

  @Test
  void confirmUsernameHash() throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    final byte[] usernameHash = TestRandomUtil.nextBytes(AccountController.USERNAME_HASH_LENGTH);

    final byte[] usernameCiphertext = TestRandomUtil.nextBytes(32);

    final byte[] zkProof = TestRandomUtil.nextBytes(32);

    final UUID linkHandle = UUID.randomUUID();

    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));

    when(accountsManager.confirmReservedUsernameHash(AUTHENTICATED_ACI, usernameHash, usernameCiphertext))
        .thenAnswer(_ -> {
          final Account updatedAccount = mock(Account.class);

          when(updatedAccount.getUsernameHash()).thenReturn(Optional.of(usernameHash));
          when(updatedAccount.getUsernameLinkHandle()).thenReturn(linkHandle);

          return updatedAccount;
        });

    final ConfirmUsernameHashResponse expectedResponse = ConfirmUsernameHashResponse.newBuilder()
        .setConfirmedUsernameHash(ConfirmUsernameHashResponse.ConfirmedUsernameHash.newBuilder()
            .setUsernameHash(ByteString.copyFrom(usernameHash))
            .setUsernameLinkHandle(UUIDUtil.toByteString(linkHandle))
            .build())
        .build();

    assertEquals(expectedResponse,
        authenticatedServiceStub().confirmUsernameHash(ConfirmUsernameHashRequest.newBuilder()
            .setUsernameHash(ByteString.copyFrom(usernameHash))
            .setUsernameCiphertext(ByteString.copyFrom(usernameCiphertext))
            .setZkProof(ByteString.copyFrom(zkProof))
            .build()));
  }

  @ParameterizedTest
  @MethodSource
  void confirmUsernameHashConfirmationException(final Exception confirmationException, final ConfirmUsernameHashResponse expectedResponse)
      throws UsernameHashNotAvailableException, UsernameReservationNotFoundException {
    final byte[] usernameHash = TestRandomUtil.nextBytes(AccountController.USERNAME_HASH_LENGTH);

    final byte[] usernameCiphertext = TestRandomUtil.nextBytes(32);

    final byte[] zkProof = TestRandomUtil.nextBytes(32);

    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));

    when(accountsManager.confirmReservedUsernameHash(any(), any(), any()))
        .thenThrow(confirmationException);

    final ConfirmUsernameHashResponse actualResponse = authenticatedServiceStub()
        .confirmUsernameHash(ConfirmUsernameHashRequest.newBuilder()
            .setUsernameHash(ByteString.copyFrom(usernameHash))
            .setUsernameCiphertext(ByteString.copyFrom(usernameCiphertext))
            .setZkProof(ByteString.copyFrom(zkProof))
            .build());

    assertEquals(expectedResponse, actualResponse);
  }

  private static Stream<Arguments> confirmUsernameHashConfirmationException() {
    return Stream.of(
        Arguments.of( new UsernameHashNotAvailableException(),
            ConfirmUsernameHashResponse.newBuilder()
                .setUsernameNotAvailable(UsernameNotAvailable.getDefaultInstance())
                .build()),
        Arguments.of(new UsernameReservationNotFoundException(),
            ConfirmUsernameHashResponse.newBuilder()
                .setReservationNotFound(FailedPrecondition.getDefaultInstance())
                .build())
    );
  }

  @Test
  void confirmUsernameHashInvalidProof() throws BaseUsernameException {
    final byte[] usernameHash = TestRandomUtil.nextBytes(AccountController.USERNAME_HASH_LENGTH);

    final byte[] usernameCiphertext = TestRandomUtil.nextBytes(32);

    final byte[] zkProof = TestRandomUtil.nextBytes(32);

    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));

    doThrow(BaseUsernameException.class).when(usernameHashZkProofVerifier).verifyProof(any(), any());

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> authenticatedServiceStub().confirmUsernameHash(ConfirmUsernameHashRequest.newBuilder()
            .setUsernameHash(ByteString.copyFrom(usernameHash))
            .setUsernameCiphertext(ByteString.copyFrom(usernameCiphertext))
            .setZkProof(ByteString.copyFrom(zkProof))
            .build()));
  }

  @ParameterizedTest
  @MethodSource
  void confirmUsernameHashInvalidArgument(final ConfirmUsernameHashRequest request) {
    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> authenticatedServiceStub().confirmUsernameHash(request));
  }

  private static List<ConfirmUsernameHashRequest> confirmUsernameHashInvalidArgument() {
    final ConfirmUsernameHashRequest prototypeRequest = ConfirmUsernameHashRequest.newBuilder()
        .setUsernameHash(ByteString.copyFrom(new byte[AccountController.USERNAME_HASH_LENGTH]))
        .setUsernameCiphertext(ByteString.copyFrom(new byte[AccountController.MAXIMUM_USERNAME_CIPHERTEXT_LENGTH]))
        .setZkProof(ByteString.copyFrom(new byte[32]))
        .build();

    return List.of(
        // No username hash
        ConfirmUsernameHashRequest.newBuilder(prototypeRequest)
            .clearUsernameHash()
            .build(),

        // Incorrect username hash length
        ConfirmUsernameHashRequest.newBuilder(prototypeRequest)
            .setUsernameHash(ByteString.copyFrom(new byte[AccountController.USERNAME_HASH_LENGTH + 1]))
            .build(),

        // No username ciphertext
        ConfirmUsernameHashRequest.newBuilder(prototypeRequest)
            .clearUsernameCiphertext()
            .build(),

        // Excessive username ciphertext length
        ConfirmUsernameHashRequest.newBuilder(prototypeRequest)
            .setUsernameCiphertext(ByteString.copyFrom(new byte[AccountController.MAXIMUM_USERNAME_CIPHERTEXT_LENGTH + 1]))
            .build(),

        // No ZK proof
        ConfirmUsernameHashRequest.newBuilder(prototypeRequest)
            .clearZkProof()
            .build());
  }

  @Test
  void deleteUsernameHash() {
    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));

    when(accountsManager.clearUsernameHash(AUTHENTICATED_ACI)).thenReturn(account);

    assertDoesNotThrow(() ->
        authenticatedServiceStub().deleteUsernameHash(DeleteUsernameHashRequest.newBuilder().build()));

    verify(accountsManager).clearUsernameHash(AUTHENTICATED_ACI);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void setUsernameLink(final boolean keepLink) {
    final Account account = mock(Account.class);
    final UUID oldHandle = UUID.randomUUID();
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(AUTHENTICATED_ACI);
    when(account.getUsernameHash()).thenReturn(Optional.of(new byte[AccountController.USERNAME_HASH_LENGTH]));
    when(account.getUsernameLinkHandle()).thenReturn(oldHandle);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));

    final byte[] usernameCiphertext = TestRandomUtil.nextBytes(EncryptedUsername.MAX_SIZE);

    final SetUsernameLinkResponse response =
        authenticatedServiceStub().setUsernameLink(SetUsernameLinkRequest.newBuilder()
            .setUsernameCiphertext(ByteString.copyFrom(usernameCiphertext))
            .setKeepLinkHandle(keepLink)
            .build());

    final ArgumentCaptor<UUID> linkHandleCaptor = ArgumentCaptor.forClass(UUID.class);

    verify(account).setUsernameLinkDetails(linkHandleCaptor.capture(), eq(usernameCiphertext));

    assertEquals(keepLink, oldHandle.equals(linkHandleCaptor.getValue()));
    final SetUsernameLinkResponse expectedResponse = SetUsernameLinkResponse.newBuilder()
        .setUsernameLinkHandle(UUIDUtil.toByteString(linkHandleCaptor.getValue()))
        .build();

    assertEquals(expectedResponse, response);
  }

  @Test
  void setUsernameLinkMissingUsernameHash() {
    final Account account = mock(Account.class);
    when(account.getUsernameHash()).thenReturn(Optional.empty());

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));

    final byte[] usernameCiphertext = TestRandomUtil.nextBytes(EncryptedUsername.MAX_SIZE);

    assertEquals(
        SetUsernameLinkResponse.newBuilder()
            .setNoUsernameSet(FailedPrecondition.getDefaultInstance())
            .build(),
        authenticatedServiceStub().setUsernameLink(SetUsernameLinkRequest.newBuilder()
            .setUsernameCiphertext(ByteString.copyFrom(usernameCiphertext))
            .build()));
  }

  @ParameterizedTest
  @MethodSource
  void setUsernameLinkIllegalCiphertext(final SetUsernameLinkRequest request) {
    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> authenticatedServiceStub().setUsernameLink(request));
  }

  private static List<SetUsernameLinkRequest> setUsernameLinkIllegalCiphertext() {
    return List.of(
        // No username ciphertext
        SetUsernameLinkRequest.newBuilder().build(),

        // Excessive username ciphertext
        SetUsernameLinkRequest.newBuilder()
            .setUsernameCiphertext(ByteString.copyFrom(new byte[EncryptedUsername.MAX_SIZE + 1]))
            .build()
    );
  }

  @Test
  void setUsernameLinkRateLimited() throws RateLimitExceededException {
    final Duration retryAfter = Duration.ofSeconds(97);

    doThrow(new RateLimitExceededException(retryAfter))
        .when(rateLimiter).validate(any(UUID.class));

    final byte[] usernameCiphertext = TestRandomUtil.nextBytes(EncryptedUsername.MAX_SIZE);

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertRateLimitExceeded(retryAfter,
        () -> authenticatedServiceStub().setUsernameLink(SetUsernameLinkRequest.newBuilder()
            .setUsernameCiphertext(ByteString.copyFrom(usernameCiphertext))
            .build()),
        accountsManager);
  }

  @Test
  void deleteUsernameLink() {
    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));

    assertDoesNotThrow(
        () -> authenticatedServiceStub().deleteUsernameLink(DeleteUsernameLinkRequest.newBuilder().build()));

    verify(account).setUsernameLinkDetails(null, null);
  }

  @Test
  void deleteUsernameLinkRateLimited() throws RateLimitExceededException {
    final Duration retryAfter = Duration.ofSeconds(11);

    doThrow(new RateLimitExceededException(retryAfter))
        .when(rateLimiter).validate(any(UUID.class));

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertRateLimitExceeded(retryAfter,
        () -> authenticatedServiceStub().deleteUsernameLink(DeleteUsernameLinkRequest.newBuilder().build()),
        accountsManager);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void configureUnidentifiedAccess(final boolean unrestrictedUnidentifiedAccess) {

    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));
    final byte[] uak = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);
    final ConfigureUnidentifiedAccessRequest.Builder builder = ConfigureUnidentifiedAccessRequest.newBuilder();
    if (unrestrictedUnidentifiedAccess) {
      builder.setAllowUnrestrictedUnidentifiedAccess(Empty.getDefaultInstance());
    } else {
      builder.setUnidentifiedAccessKey(ByteString.copyFrom(uak));
    }

    assertDoesNotThrow(() -> authenticatedServiceStub().configureUnidentifiedAccess(builder.build()));

    verify(account).setUnrestrictedUnidentifiedAccess(unrestrictedUnidentifiedAccess);
    verify(account).setUnidentifiedAccessKey(unrestrictedUnidentifiedAccess ? null : uak);
  }

  @ParameterizedTest
  @MethodSource
  void configureUnidentifiedAccessIllegalArguments(final ConfigureUnidentifiedAccessRequest request) {
    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> authenticatedServiceStub().configureUnidentifiedAccess(request));
  }

  private static List<ConfigureUnidentifiedAccessRequest> configureUnidentifiedAccessIllegalArguments() {
    return List.of(
        // No key and no unrestricted unidentified access
        ConfigureUnidentifiedAccessRequest.newBuilder().build(),

        // Key with incorrect length
        ConfigureUnidentifiedAccessRequest.newBuilder()
            .setUnidentifiedAccessKey(ByteString.copyFrom(new byte[15]))
            .build()
    );
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void setDiscoverableByPhoneNumber(final boolean discoverableByPhoneNumber) {
    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));

    assertDoesNotThrow(() ->
        authenticatedServiceStub().setDiscoverableByPhoneNumber(SetDiscoverableByPhoneNumberRequest.newBuilder()
            .setDiscoverableByPhoneNumber(discoverableByPhoneNumber)
            .build()));

    verify(account).setDiscoverableByPhoneNumber(discoverableByPhoneNumber);
  }

  @Test
  void setRegistrationRecoveryPassword() {
    final UUID phoneNumberIdentifier = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.PNI)).thenReturn(phoneNumberIdentifier);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));

    final byte[] registrationRecoveryPassword = TestRandomUtil.nextBytes(32);

    assertDoesNotThrow(() ->
        authenticatedServiceStub().setRegistrationRecoveryPassword(SetRegistrationRecoveryPasswordRequest.newBuilder()
            .setRegistrationRecoveryPassword(ByteString.copyFrom(registrationRecoveryPassword))
            .build()));

    verify(registrationRecoveryPasswordsManager).store(account.getIdentifier(IdentityType.PNI), registrationRecoveryPassword);
  }

  @Test
  void setRegistrationRecoveryPasswordMissingPassword() {
    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> authenticatedServiceStub().setRegistrationRecoveryPassword(
            SetRegistrationRecoveryPasswordRequest.newBuilder().build()));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void setZkCredentialKey(final boolean matchesCurrentZkCredentialKey) throws Exception {

    final ZkCredentialPublicKey publicKey = ZkCredentialKeyPair.generate().getPublicKey();
    final long rotationId = ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE);

    final Account account = mock(Account.class);

    if (matchesCurrentZkCredentialKey) {
      when(account.getZkCredentialKey()).thenReturn(Optional.of(publicKey));
      when(account.getZkCredentialKeyRotationId()).thenReturn(rotationId);
    }

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));

    final SetZkCredentialKeyResponse response = assertDoesNotThrow(() ->
        authenticatedServiceStub().setZkCredentialKey(SetZkCredentialKeyRequest.newBuilder()
                .setPublicKey(ByteString.copyFrom(publicKey.serialize()))
            .build()));

    if (matchesCurrentZkCredentialKey) {
      assertEquals(rotationId, response.getRotationId());
    } else {
      assertTrue(response.getRotationId() != 0);
    }

    final int updateMethodCalls = matchesCurrentZkCredentialKey ? 0 : 1;

    verify(accountsManager, times(updateMethodCalls)).update(eq(AUTHENTICATED_ACI), any());
    verify(account, times(updateMethodCalls)).setZkCredentialKey(publicKey);
  }

  @Test
  void setZkCredentialKeyRateLimited() throws Exception {

    final byte[] publicKey = ZkCredentialKeyPair.generate().getPublicKey().serialize();
    final Duration retryDuration = Duration.ofDays(1);

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(AUTHENTICATED_ACI);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI))
        .thenReturn(Optional.of(account));
    doThrow(new RateLimitExceededException(retryDuration))
        .when(rateLimiter).validate(AUTHENTICATED_ACI);

    GrpcTestUtils.assertRateLimitExceeded(retryDuration, () ->
        authenticatedServiceStub().setZkCredentialKey(SetZkCredentialKeyRequest.newBuilder()
            .setPublicKey(ByteString.copyFrom(publicKey))
            .build()));

    verify(accountsManager, never()).update(any(UUID.class), any());
    verify(account, never()).setZkCredentialKey(any());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void getEntitlements(final boolean expired) {
    final Account account = mock(Account.class);
    final Instant expiration = expired
        ? testClock.instant().minus(Duration.ofDays(1))
        : testClock.instant().plus(Duration.ofMillis(1));
    final AccountBadge badge1 = new AccountBadge("badge1", expiration, true);
    final AccountBadge badge2 = new AccountBadge("badge2", expiration, true);

    when(account.getBackupVoucher()).thenReturn(new Account.BackupVoucher(100, expiration));
    when(account.getBadges()).thenReturn(List.of(badge1, badge2));
    when(account.getUuid()).thenReturn(AUTHENTICATED_ACI);
    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI)).thenReturn(Optional.of(account));

    final GetEntitlementsResponse entitlements = authenticatedServiceStub()
        .getEntitlements(GetEntitlementsRequest.newBuilder().build());

    if (expired) {
      assertThat(entitlements.getBadgesCount()).isEqualTo(0);
      assertThat(entitlements.hasBackup()).isFalse();
    } else {
      assertThat(entitlements.getBadges(0)).isEqualTo(toBadgeEntitlement(badge1));
      assertThat(entitlements.getBadges(1)).isEqualTo(toBadgeEntitlement(badge2));
      assertThat(entitlements.getBackup().getLevel()).isEqualTo(100);
      assertThat(entitlements.getBackup().getExpirationEpochSeconds()).isEqualTo(expiration.getEpochSecond());
    }
  }

  private static GetEntitlementsResponse.BadgeEntitlement toBadgeEntitlement(AccountBadge badge) {
    return GetEntitlementsResponse.BadgeEntitlement.newBuilder()
        .setExpirationEpochSeconds(badge.expiration().getEpochSecond())
        .setBadgeId(badge.id())
        .setVisible(badge.visible())
        .build();
  }

  @Test
  void changeNumber() throws Exception {
    final String newNumber = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());

    final ECSignedPreKey ecSignedPreKey = KeysHelper.signedECPreKey(1, pniIdentityKeyPair);
    final KEMSignedPreKey kemSignedPreKey = KeysHelper.signedKEMPreKey(2, pniIdentityKeyPair);

    final byte[] sessionId = TestRandomUtil.nextBytes(16);
    final UUID updatedPni = UUID.randomUUID();

    final Account updatedAccount = mock(Account.class);
    when(updatedAccount.getUuid()).thenReturn(AUTHENTICATED_ACI);
    when(updatedAccount.getNumber()).thenReturn(newNumber);
    when(updatedAccount.getPhoneNumberIdentifier()).thenReturn(updatedPni);
    when(updatedAccount.getUsernameHash()).thenReturn(Optional.empty());

    when(changeNumberManager.changeNumber(eq(AUTHENTICATED_ACI), any(), any(), any(), eq(newNumber),
        any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(updatedAccount);

    final ChangeNumberResponse response = authenticatedServiceStub().changeNumber(ChangeNumberRequest.newBuilder()
        .setSessionId(ByteString.copyFrom(sessionId))
        .setNumber(newNumber)
        .setRegistrationLock(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
        .setPniIdentityKey(ByteString.copyFrom(pniIdentityKey.serialize()))
        .putDevicePniSignedPreKeys(Device.PRIMARY_ID, EcSignedPreKey.newBuilder()
            .setKeyId(KeyIdUtil.toUnsignedInt(ecSignedPreKey.keyId()))
            .setPublicKey(ByteString.copyFrom(ecSignedPreKey.serializedPublicKey()))
            .setSignature(ByteString.copyFrom(ecSignedPreKey.signature()))
            .build())
        .putDevicePniPqLastResortPreKeys(Device.PRIMARY_ID, KemSignedPreKey.newBuilder()
            .setKeyId(KeyIdUtil.toUnsignedInt(kemSignedPreKey.keyId()))
            .setPublicKey(ByteString.copyFrom(kemSignedPreKey.serializedPublicKey()))
            .setSignature(ByteString.copyFrom(kemSignedPreKey.signature()))
            .build())
        .putPniRegistrationIds(Device.PRIMARY_ID, 17)
        .build());

    final ChangeNumberResponse expectedResponse = ChangeNumberResponse.newBuilder()
        .setAccountIdentifiers(AccountIdentifiers.newBuilder()
            .addServiceIdentifiers(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(AUTHENTICATED_ACI)))
            .addServiceIdentifiers(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(new PniServiceIdentifier(updatedPni)))
            .setE164(newNumber))
        .build();

    assertEquals(expectedResponse, response);
  }

  @ParameterizedTest
  @MethodSource
  void changeNumberErrorResponse(final Exception exceptionToThrow, final ChangeNumberResponse expectedResponse)
      throws Exception {

    when(changeNumberManager.changeNumber(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(exceptionToThrow);

    assertEquals(expectedResponse, authenticatedServiceStub().changeNumber(createChangeNumberRequest()));
  }

  private static List<Arguments> changeNumberErrorResponse() {
    return List.of(
        Arguments.argumentSet("Invalid registration session",
            new InvalidRegistrationSessionException("invalid registration session"),
            ChangeNumberResponse.newBuilder()
                .setInvalidRegistrationSession(FailedPrecondition.newBuilder().setDescription("invalid registration session"))
                .build()),
        Arguments.argumentSet("Unverified registration session",
            new UnverifiedRegistrationSessionException(),
            ChangeNumberResponse.newBuilder()
                .setUnverifiedRegistrationSession(FailedPrecondition.getDefaultInstance())
                .build()),
        Arguments.argumentSet("Recovery password verification failed",
            new RecoveryPasswordVerificationFailedException(),
            ChangeNumberResponse.newBuilder()
                .setRecoveryPasswordVerificationFailed(FailedPrecondition.getDefaultInstance())
                .build()),
        Arguments.argumentSet("Message too large",
            new MessageTooLargeException(),
            ChangeNumberResponse.newBuilder()
                .setMessageTooLarge(FailedPrecondition.newBuilder().setDescription("one or more device messages was too large"))
                .build()));
  }

  @ParameterizedTest
  @MethodSource
  void changeNumberUnavailable(final Exception exceptionToThrow) throws Exception {
    when(changeNumberManager.changeNumber(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(exceptionToThrow);

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.UNAVAILABLE,
        () -> authenticatedServiceStub().changeNumber(createChangeNumberRequest()));
  }

  private static List<Arguments> changeNumberUnavailable() {
    return List.of(
        Arguments.argumentSet("Message delivery not allowed", new MessageDeliveryNotAllowedException()),
        Arguments.argumentSet("Registration service unavailable", new IOException("unavailable")));
  }

  @Test
  void changeNumberRegistrationLockFailure() throws Exception {
    final long timeRemaining = Duration.ofDays(7).toMillis();

    when(changeNumberManager.changeNumber(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(new RegistrationLockFailureException(new org.whispersystems.textsecuregcm.entities.RegistrationLockFailure(
            timeRemaining,
            new org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials("test-username", "test-password"))));

    final ChangeNumberResponse expectedResponse = ChangeNumberResponse.newBuilder()
        .setRegistrationLockFailure(RegistrationLockFailure.newBuilder()
            .setTimeRemainingMillis(timeRemaining)
            .setSvr2Credentials(ExternalServiceCredentials.newBuilder()
                .setUsername("test-username")
                .setPassword("test-password")))
        .build();

    assertEquals(expectedResponse, authenticatedServiceStub().changeNumber(createChangeNumberRequest()));
  }

  @Test
  void changeNumberStaleDevices() throws Exception {
    final byte staleDeviceId = (byte) (Device.PRIMARY_ID + 1);

    when(changeNumberManager.changeNumber(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(new MismatchedDevicesException(new MismatchedDevices(Set.of(), Set.of(), Set.of(staleDeviceId))));

    final ChangeNumberResponse expectedResponse = ChangeNumberResponse.newBuilder()
        .setStaleDevices(StaleDevices.newBuilder().addStaleDevices(staleDeviceId))
        .build();

    assertEquals(expectedResponse, authenticatedServiceStub().changeNumber(createChangeNumberRequest()));
  }

  @Test
  void changeNumberMismatchedDevices() throws Exception {
    final byte missingDeviceId = (byte) (Device.PRIMARY_ID + 1);
    final byte extraDeviceId = (byte) (Device.PRIMARY_ID + 2);

    when(changeNumberManager.changeNumber(any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenThrow(new MismatchedDevicesException(new MismatchedDevices(Set.of(missingDeviceId), Set.of(extraDeviceId), Set.of())));

    final ChangeNumberResponse expectedResponse = ChangeNumberResponse.newBuilder()
        .setMismatchedDevices(org.signal.chat.messages.MismatchedDevices.newBuilder()
            .setServiceIdentifier(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(AUTHENTICATED_ACI)))
            .addMissingDevices(missingDeviceId)
            .addExtraDevices(extraDeviceId))
        .build();

    assertEquals(expectedResponse, authenticatedServiceStub().changeNumber(createChangeNumberRequest()));
  }

  private static ChangeNumberRequest createChangeNumberRequest() {
    final ECKeyPair pniIdentityKeyPair = ECKeyPair.generate();
    final IdentityKey pniIdentityKey = new IdentityKey(pniIdentityKeyPair.getPublicKey());

    final ECSignedPreKey ecSignedPreKey = KeysHelper.signedECPreKey(1, pniIdentityKeyPair);
    final KEMSignedPreKey kemSignedPreKey = KeysHelper.signedKEMPreKey(2, pniIdentityKeyPair);

    return ChangeNumberRequest.newBuilder()
        .setSessionId(ByteString.copyFrom(TestRandomUtil.nextBytes(16)))
        .setNumber(PhoneNumberUtil.getInstance().format(
            PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164))
        .setPniIdentityKey(ByteString.copyFrom(pniIdentityKey.serialize()))
        .putDevicePniSignedPreKeys(Device.PRIMARY_ID, EcSignedPreKey.newBuilder()
            .setKeyId(KeyIdUtil.toUnsignedInt(ecSignedPreKey.keyId()))
            .setPublicKey(ByteString.copyFrom(ecSignedPreKey.serializedPublicKey()))
            .setSignature(ByteString.copyFrom(ecSignedPreKey.signature()))
            .build())
        .putDevicePniPqLastResortPreKeys(Device.PRIMARY_ID, KemSignedPreKey.newBuilder()
            .setKeyId(KeyIdUtil.toUnsignedInt(kemSignedPreKey.keyId()))
            .setPublicKey(ByteString.copyFrom(kemSignedPreKey.serializedPublicKey()))
            .setSignature(ByteString.copyFrom(kemSignedPreKey.signature()))
            .build())
        .putPniRegistrationIds(Device.PRIMARY_ID, 17)
        .build();
  }

  @ParameterizedTest
  @ArgumentsSource(AccountsTestHelper.AccountsDataReportArgumentProvider.class)
  void getAccountDataReport(final Account account, final String expectedTextAfterHeader) {
    getMockAuthenticationInterceptor().setAuthenticatedDevice(account.getUuid(), Device.PRIMARY_ID);
    when(accountsManager.getByAccountIdentifier(account.getUuid())).thenReturn(Optional.of(account));

    final GetAccountDataReportResponse response =
        authenticatedServiceStub().getAccountDataReport(GetAccountDataReportRequest.newBuilder().build());

    final String actualText = response.getText();
    AccountsTestHelper.verifyAccountDataReportText(actualText, expectedTextAfterHeader);
  }

  @Test
  void getCapabilities() {
    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(AUTHENTICATED_ACI);

    // public-visible and self-visible capabilities should be returned on the authenticated RPC
    when(account.hasCapability(DeviceCapability.SPARSE_POST_QUANTUM_RATCHET)).thenReturn(true);
    when(account.hasCapability(DeviceCapability.USERNAME_CHANGE_SYNC_MESSAGE)).thenReturn(true);
    when(account.getUuid()).thenReturn(AUTHENTICATED_ACI);

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI)).thenReturn(Optional.of(account));

    final GetCapabilitiesResponse response = authenticatedServiceStub()
        .getCapabilities(GetCapabilitiesRequest.getDefaultInstance());

    assertEquals(Capabilities.newBuilder()
        .addCapabilities(org.signal.chat.common.DeviceCapability.DEVICE_CAPABILITY_SPARSE_POST_QUANTUM_RATCHET)
        .addCapabilities(org.signal.chat.common.DeviceCapability.DEVICE_CAPABILITY_USERNAME_CHANGE_SYNC_MESSAGE)
        .build(), response.getCapabilities());
  }
}
