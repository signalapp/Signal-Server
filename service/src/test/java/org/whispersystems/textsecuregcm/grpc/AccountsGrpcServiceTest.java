/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.time.Duration;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.signal.chat.account.AccountsGrpc;
import org.signal.chat.account.ClearRegistrationLockRequest;
import org.signal.chat.account.ClearRegistrationLockResponse;
import org.signal.chat.account.ConfigureUnidentifiedAccessRequest;
import org.signal.chat.account.ConfirmUsernameHashRequest;
import org.signal.chat.account.ConfirmUsernameHashResponse;
import org.signal.chat.account.DeleteAccountRequest;
import org.signal.chat.account.DeleteAccountResponse;
import org.signal.chat.account.DeleteUsernameHashRequest;
import org.signal.chat.account.DeleteUsernameLinkRequest;
import org.signal.chat.account.GetAccountIdentityRequest;
import org.signal.chat.account.GetAccountIdentityResponse;
import org.signal.chat.account.ReserveUsernameHashError;
import org.signal.chat.account.ReserveUsernameHashErrorType;
import org.signal.chat.account.ReserveUsernameHashRequest;
import org.signal.chat.account.ReserveUsernameHashResponse;
import org.signal.chat.account.SetDiscoverableByPhoneNumberRequest;
import org.signal.chat.account.SetRegistrationLockRequest;
import org.signal.chat.account.SetRegistrationLockResponse;
import org.signal.chat.account.SetRegistrationRecoveryPasswordRequest;
import org.signal.chat.account.SetUsernameLinkRequest;
import org.signal.chat.account.SetUsernameLinkResponse;
import org.signal.chat.common.AccountIdentifiers;
import org.signal.libsignal.usernames.BaseUsernameException;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.EncryptedUsername;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.UsernameHashNotAvailableException;
import org.whispersystems.textsecuregcm.storage.UsernameReservationNotFoundException;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import org.whispersystems.textsecuregcm.util.UsernameHashZkProofVerifier;
import reactor.core.publisher.Mono;

class AccountsGrpcServiceTest extends SimpleBaseGrpcTest<AccountsGrpcService, AccountsGrpc.AccountsBlockingStub> {

  @Mock
  private AccountsManager accountsManager;

  @Mock
  private RateLimiter rateLimiter;

  @Mock
  private UsernameHashZkProofVerifier usernameHashZkProofVerifier;

  @Mock
  private RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;

  @Override
  protected AccountsGrpcService createServiceBeforeEachTest() {
    when(accountsManager.updateAsync(any(), any()))
        .thenAnswer(invocation -> {
          final Account account = invocation.getArgument(0);
          final Consumer<Account> updater = invocation.getArgument(1);

          updater.accept(account);

          return CompletableFuture.completedFuture(account);
        });

    final RateLimiters rateLimiters = mock(RateLimiters.class);
    when(rateLimiters.getUsernameReserveLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getUsernameSetLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getUsernameLinkOperationLimiter()).thenReturn(rateLimiter);

    when(rateLimiter.validateReactive(any(UUID.class))).thenReturn(Mono.empty());
    when(rateLimiter.validateReactive(anyString())).thenReturn(Mono.empty());

    when(registrationRecoveryPasswordsManager.store(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    return new AccountsGrpcService(accountsManager,
        rateLimiters,
        usernameHashZkProofVerifier,
        registrationRecoveryPasswordsManager);
  }

  @Test
  void getAccountIdentity() {
    final UUID phoneNumberIdentifier = UUID.randomUUID();
    final String e164 = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164);

    final byte[] usernameHash = TestRandomUtil.nextBytes(32);

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(AUTHENTICATED_ACI);
    when(account.getPhoneNumberIdentifier()).thenReturn(phoneNumberIdentifier);
    when(account.getNumber()).thenReturn(e164);
    when(account.getUsernameHash()).thenReturn(Optional.of(usernameHash));

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    final GetAccountIdentityResponse expectedResponse = GetAccountIdentityResponse.newBuilder()
        .setAccountIdentifiers(AccountIdentifiers.newBuilder()
            .addServiceIdentifiers(ServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(AUTHENTICATED_ACI)))
            .addServiceIdentifiers(ServiceIdentifierUtil.toGrpcServiceIdentifier(new PniServiceIdentifier(phoneNumberIdentifier)))
            .setE164(e164)
            .setUsernameHash(ByteString.copyFrom(usernameHash))
            .build())
        .build();

    assertEquals(expectedResponse, authenticatedServiceStub().getAccountIdentity(GetAccountIdentityRequest.newBuilder().build()));
  }

  @Test
  void deleteAccount() {
    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    when(accountsManager.delete(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final DeleteAccountResponse ignored =
        authenticatedServiceStub().deleteAccount(DeleteAccountRequest.newBuilder().build());

    verify(accountsManager).delete(account, AccountsManager.DeletionReason.USER_REQUEST);
  }

  @Test
  void deleteAccountLinkedDevice() {
    getMockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, (byte) (Device.PRIMARY_ID + 1));

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.PERMISSION_DENIED,
        () -> authenticatedServiceStub().deleteAccount(DeleteAccountRequest.newBuilder().build()));

    verify(accountsManager, never()).delete(any(), any());
  }

  @Test
  void setRegistrationLock() {
    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

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

    verify(accountsManager, never()).updateAsync(any(), any());
  }

  @Test
  void setRegistrationLockLinkedDevice() {
    getMockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, (byte) (Device.PRIMARY_ID + 1));

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.PERMISSION_DENIED,
        () -> authenticatedServiceStub().setRegistrationLock(SetRegistrationLockRequest.newBuilder()
            .build()));

    verify(accountsManager, never()).updateAsync(any(), any());
  }

  @Test
  void clearRegistrationLock() {
    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    final ClearRegistrationLockResponse ignored =
        authenticatedServiceStub().clearRegistrationLock(ClearRegistrationLockRequest.newBuilder().build());

    verify(account).setRegistrationLock(null, null);
  }

  @Test
  void clearRegistrationLockLinkedDevice() {
    getMockAuthenticationInterceptor().setAuthenticatedDevice(AUTHENTICATED_ACI, (byte) (Device.PRIMARY_ID + 1));

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.PERMISSION_DENIED,
        () -> authenticatedServiceStub().clearRegistrationLock(ClearRegistrationLockRequest.newBuilder().build()));

    verify(accountsManager, never()).updateAsync(any(), any());
  }

  @Test
  void reserveUsernameHash() {
    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    final byte[] usernameHash = TestRandomUtil.nextBytes(AccountController.USERNAME_HASH_LENGTH);

    when(accountsManager.reserveUsernameHash(any(), any()))
        .thenAnswer(invocation -> {
          final List<byte[]> usernameHashes = invocation.getArgument(1);

          return CompletableFuture.completedFuture(
              new AccountsManager.UsernameReservation(invocation.getArgument(0), usernameHashes.getFirst()));
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
  void reserveUsernameHashNotAvailable() {
    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    final byte[] usernameHash = TestRandomUtil.nextBytes(AccountController.USERNAME_HASH_LENGTH);

    when(accountsManager.reserveUsernameHash(any(), any()))
        .thenReturn(CompletableFuture.failedFuture(new UsernameHashNotAvailableException()));

    final ReserveUsernameHashResponse expectedResponse = ReserveUsernameHashResponse.newBuilder()
        .setError(ReserveUsernameHashError.newBuilder()
            .setErrorType(ReserveUsernameHashErrorType.RESERVE_USERNAME_HASH_ERROR_TYPE_NO_HASHES_AVAILABLE)
            .build())
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

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    final byte[] usernameHash = TestRandomUtil.nextBytes(AccountController.USERNAME_HASH_LENGTH + 1);

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> authenticatedServiceStub().reserveUsernameHash(ReserveUsernameHashRequest.newBuilder()
            .addUsernameHashes(ByteString.copyFrom(usernameHash))
            .build()));
  }

  @Test
  void reserveUsernameHashRateLimited() {
    final byte[] usernameHash = TestRandomUtil.nextBytes(AccountController.USERNAME_HASH_LENGTH);

    final Duration retryAfter = Duration.ofMinutes(3);

    when(rateLimiter.validateReactive(any(UUID.class)))
        .thenReturn(Mono.error(new RateLimitExceededException(retryAfter)));

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertRateLimitExceeded(retryAfter,
        () -> authenticatedServiceStub().reserveUsernameHash(ReserveUsernameHashRequest.newBuilder()
            .addUsernameHashes(ByteString.copyFrom(usernameHash))
            .build()),
        accountsManager);
  }

  @Test
  void confirmUsernameHash() {
    final byte[] usernameHash = TestRandomUtil.nextBytes(AccountController.USERNAME_HASH_LENGTH);

    final byte[] usernameCiphertext = TestRandomUtil.nextBytes(32);

    final byte[] zkProof = TestRandomUtil.nextBytes(32);

    final UUID linkHandle = UUID.randomUUID();

    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    when(accountsManager.confirmReservedUsernameHash(account, usernameHash, usernameCiphertext))
        .thenAnswer(invocation -> {
          final Account updatedAccount = mock(Account.class);

          when(updatedAccount.getUsernameHash()).thenReturn(Optional.of(usernameHash));
          when(updatedAccount.getUsernameLinkHandle()).thenReturn(linkHandle);

          return CompletableFuture.completedFuture(updatedAccount);
        });

    final ConfirmUsernameHashResponse expectedResponse = ConfirmUsernameHashResponse.newBuilder()
        .setUsernameHash(ByteString.copyFrom(usernameHash))
        .setUsernameLinkHandle(UUIDUtil.toByteString(linkHandle))
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
  void confirmUsernameHashConfirmationException(final Exception confirmationException, final Status expectedStatus) {
    final byte[] usernameHash = TestRandomUtil.nextBytes(AccountController.USERNAME_HASH_LENGTH);

    final byte[] usernameCiphertext = TestRandomUtil.nextBytes(32);

    final byte[] zkProof = TestRandomUtil.nextBytes(32);

    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    when(accountsManager.confirmReservedUsernameHash(any(), any(), any()))
        .thenReturn(CompletableFuture.failedFuture(confirmationException));

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(expectedStatus,
        () -> authenticatedServiceStub().confirmUsernameHash(ConfirmUsernameHashRequest.newBuilder()
            .setUsernameHash(ByteString.copyFrom(usernameHash))
            .setUsernameCiphertext(ByteString.copyFrom(usernameCiphertext))
            .setZkProof(ByteString.copyFrom(zkProof))
            .build()));
  }

  private static Stream<Arguments> confirmUsernameHashConfirmationException() {
    return Stream.of(
        Arguments.of(new UsernameHashNotAvailableException(), Status.NOT_FOUND),
        Arguments.of(new UsernameReservationNotFoundException(), Status.FAILED_PRECONDITION)
    );
  }

  @Test
  void confirmUsernameHashInvalidProof() throws BaseUsernameException {
    final byte[] usernameHash = TestRandomUtil.nextBytes(AccountController.USERNAME_HASH_LENGTH);

    final byte[] usernameCiphertext = TestRandomUtil.nextBytes(32);

    final byte[] zkProof = TestRandomUtil.nextBytes(32);

    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

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

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    when(accountsManager.clearUsernameHash(account)).thenReturn(CompletableFuture.completedFuture(account));

    assertDoesNotThrow(() ->
        authenticatedServiceStub().deleteUsernameHash(DeleteUsernameHashRequest.newBuilder().build()));

    verify(accountsManager).clearUsernameHash(account);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void setUsernameLink(final boolean keepLink) {
    final Account account = mock(Account.class);
    final UUID oldHandle = UUID.randomUUID();
    when(account.getUsernameHash()).thenReturn(Optional.of(new byte[AccountController.USERNAME_HASH_LENGTH]));
    when(account.getUsernameLinkHandle()).thenReturn(oldHandle);

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

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

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    final byte[] usernameCiphertext = TestRandomUtil.nextBytes(EncryptedUsername.MAX_SIZE);

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.FAILED_PRECONDITION,
        () -> authenticatedServiceStub().setUsernameLink(SetUsernameLinkRequest.newBuilder()
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
  void setUsernameLinkRateLimited() {
    final Duration retryAfter = Duration.ofSeconds(97);

    when(rateLimiter.validateReactive(any(UUID.class)))
        .thenReturn(Mono.error(new RateLimitExceededException(retryAfter)));

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

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    assertDoesNotThrow(
        () -> authenticatedServiceStub().deleteUsernameLink(DeleteUsernameLinkRequest.newBuilder().build()));

    verify(account).setUsernameLinkDetails(null, null);
  }

  @Test
  void deleteUsernameLinkRateLimited() {
    final Duration retryAfter = Duration.ofSeconds(11);

    when(rateLimiter.validateReactive(any(UUID.class)))
        .thenReturn(Mono.error(new RateLimitExceededException(retryAfter)));

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertRateLimitExceeded(retryAfter,
        () -> authenticatedServiceStub().deleteUsernameLink(DeleteUsernameLinkRequest.newBuilder().build()),
        accountsManager);
  }

  @ParameterizedTest
  @MethodSource
  void configureUnidentifiedAccess(final boolean unrestrictedUnidentifiedAccess,
      final byte[] unidentifiedAccessKey,
      final byte[] expectedUnidentifiedAccessKey) {

    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    assertDoesNotThrow(() -> authenticatedServiceStub().configureUnidentifiedAccess(ConfigureUnidentifiedAccessRequest.newBuilder()
        .setAllowUnrestrictedUnidentifiedAccess(unrestrictedUnidentifiedAccess)
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .build()));

    verify(account).setUnrestrictedUnidentifiedAccess(unrestrictedUnidentifiedAccess);
    verify(account).setUnidentifiedAccessKey(expectedUnidentifiedAccessKey);
  }

  private static Stream<Arguments> configureUnidentifiedAccess() {
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    return Stream.of(
        Arguments.of(true, new byte[0], null),
        Arguments.of(true, unidentifiedAccessKey, null),
        Arguments.of(false, unidentifiedAccessKey, unidentifiedAccessKey)
    );
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
            .setAllowUnrestrictedUnidentifiedAccess(false)
            .setUnidentifiedAccessKey(ByteString.copyFrom(new byte[15]))
            .build()
    );
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void setDiscoverableByPhoneNumber(final boolean discoverableByPhoneNumber) {
    final Account account = mock(Account.class);

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

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

    when(accountsManager.getByAccountIdentifierAsync(AUTHENTICATED_ACI))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

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
}
