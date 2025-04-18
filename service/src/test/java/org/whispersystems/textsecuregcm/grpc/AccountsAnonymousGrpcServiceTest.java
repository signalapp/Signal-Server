/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.signal.chat.account.AccountsAnonymousGrpc;
import org.signal.chat.account.CheckAccountExistenceRequest;
import org.signal.chat.account.LookupUsernameHashRequest;
import org.signal.chat.account.LookupUsernameLinkRequest;
import org.signal.chat.common.IdentityType;
import org.signal.chat.common.ServiceIdentifier;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import reactor.core.publisher.Mono;

class AccountsAnonymousGrpcServiceTest extends
    SimpleBaseGrpcTest<AccountsAnonymousGrpcService, AccountsAnonymousGrpc.AccountsAnonymousBlockingStub> {

  @Mock
  private AccountsManager accountsManager;

  @Mock
  private RateLimiters rateLimiters;

  @Mock
  private RateLimiter rateLimiter;

  @Override
  protected AccountsAnonymousGrpcService createServiceBeforeEachTest() {
    when(accountsManager.getByServiceIdentifierAsync(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    when(accountsManager.getByUsernameHash(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    when(accountsManager.getByUsernameLinkHandle(any()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    when(rateLimiters.getCheckAccountExistenceLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getUsernameLookupLimiter()).thenReturn(rateLimiter);
    when(rateLimiters.getUsernameLinkLookupLimiter()).thenReturn(rateLimiter);

    when(rateLimiter.validateReactive(anyString())).thenReturn(Mono.empty());

    getMockRequestAttributesInterceptor().setRequestAttributes(
        new RequestAttributes(InetAddresses.forString("127.0.0.1"), null, null));

    return new AccountsAnonymousGrpcService(accountsManager, rateLimiters);
  }

  @Test
  void checkAccountExistence() {
    final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

    when(accountsManager.getByServiceIdentifierAsync(serviceIdentifier))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(mock(Account.class))));

    assertTrue(unauthenticatedServiceStub().checkAccountExistence(CheckAccountExistenceRequest.newBuilder()
        .setServiceIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier))
        .build()).getAccountExists());

    assertFalse(unauthenticatedServiceStub().checkAccountExistence(CheckAccountExistenceRequest.newBuilder()
        .setServiceIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(UUID.randomUUID())))
        .build()).getAccountExists());
  }

  @ParameterizedTest
  @MethodSource
  void checkAccountExistenceIllegalRequest(final CheckAccountExistenceRequest request) {
    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> unauthenticatedServiceStub().checkAccountExistence(request));
  }

  private static Stream<Arguments> checkAccountExistenceIllegalRequest() {
    return Stream.of(
        // No service identifier
        Arguments.of(CheckAccountExistenceRequest.newBuilder().build()),

        // Bad service identifier
        Arguments.of(CheckAccountExistenceRequest.newBuilder()
            .setServiceIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(ByteString.copyFrom(new byte[15]))
                .build())
            .build())
    );
  }

  @Test
  void checkAccountExistenceRateLimited() {
    final Duration retryAfter = Duration.ofSeconds(11);

    when(rateLimiter.validateReactive(anyString()))
        .thenReturn(Mono.error(new RateLimitExceededException(retryAfter)));

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertRateLimitExceeded(retryAfter,
        () -> unauthenticatedServiceStub().checkAccountExistence(CheckAccountExistenceRequest.newBuilder()
            .setServiceIdentifier(ServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(UUID.randomUUID())))
            .build()),
        accountsManager);
  }

  @Test
  void lookupUsernameHash() {
    final UUID accountIdentifier = UUID.randomUUID();

    final byte[] usernameHash = TestRandomUtil.nextBytes(AccountController.USERNAME_HASH_LENGTH);

    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(accountIdentifier);

    when(accountsManager.getByUsernameHash(usernameHash))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    assertEquals(ServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(accountIdentifier)),
        unauthenticatedServiceStub().lookupUsernameHash(LookupUsernameHashRequest.newBuilder()
                .setUsernameHash(ByteString.copyFrom(usernameHash))
                .build())
            .getServiceIdentifier());

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.NOT_FOUND,
        () -> unauthenticatedServiceStub().lookupUsernameHash(LookupUsernameHashRequest.newBuilder()
            .setUsernameHash(ByteString.copyFrom(new byte[AccountController.USERNAME_HASH_LENGTH]))
            .build()));
  }

  @ParameterizedTest
  @MethodSource
  void lookupUsernameHashIllegalHash(final LookupUsernameHashRequest request) {
    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> unauthenticatedServiceStub().lookupUsernameHash(request));
  }

  private static Stream<Arguments> lookupUsernameHashIllegalHash() {
    return Stream.of(
        // No username hash
        Arguments.of(LookupUsernameHashRequest.newBuilder().build()),

        // Hash too long
        Arguments.of(LookupUsernameHashRequest.newBuilder()
            .setUsernameHash(ByteString.copyFrom(new byte[AccountController.USERNAME_HASH_LENGTH + 1]))
            .build()),

        // Hash too short
        Arguments.of(LookupUsernameHashRequest.newBuilder()
            .setUsernameHash(ByteString.copyFrom(new byte[AccountController.USERNAME_HASH_LENGTH - 1]))
            .build())
    );
  }

  @Test
  void lookupUsernameHashRateLimited() {
    final Duration retryAfter = Duration.ofSeconds(13);

    when(rateLimiter.validateReactive(anyString()))
        .thenReturn(Mono.error(new RateLimitExceededException(retryAfter)));

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertRateLimitExceeded(retryAfter,
        () -> unauthenticatedServiceStub().lookupUsernameHash(LookupUsernameHashRequest.newBuilder()
            .setUsernameHash(ByteString.copyFrom(new byte[AccountController.USERNAME_HASH_LENGTH]))
            .build()),
        accountsManager);
  }

  @Test
  void lookupUsernameLink() {
    final UUID linkHandle = UUID.randomUUID();

    final byte[] usernameCiphertext = TestRandomUtil.nextBytes(32);

    final Account account = mock(Account.class);
    when(account.getEncryptedUsername()).thenReturn(Optional.of(usernameCiphertext));

    when(accountsManager.getByUsernameLinkHandle(linkHandle))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    assertEquals(ByteString.copyFrom(usernameCiphertext),
        unauthenticatedServiceStub().lookupUsernameLink(LookupUsernameLinkRequest.newBuilder()
                .setUsernameLinkHandle(UUIDUtil.toByteString(linkHandle))
                .build())
            .getUsernameCiphertext());

    when(account.getEncryptedUsername()).thenReturn(Optional.empty());

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.NOT_FOUND,
        () -> unauthenticatedServiceStub().lookupUsernameLink(LookupUsernameLinkRequest.newBuilder()
            .setUsernameLinkHandle(UUIDUtil.toByteString(linkHandle))
            .build()));

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.NOT_FOUND,
        () -> unauthenticatedServiceStub().lookupUsernameLink(LookupUsernameLinkRequest.newBuilder()
            .setUsernameLinkHandle(UUIDUtil.toByteString(UUID.randomUUID()))
            .build()));
  }

  @ParameterizedTest
  @MethodSource
  void lookupUsernameLinkIllegalHandle(final LookupUsernameLinkRequest request) {
    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> unauthenticatedServiceStub().lookupUsernameLink(request));
  }

  private static Stream<Arguments> lookupUsernameLinkIllegalHandle() {
    return Stream.of(
        // No handle
        Arguments.of(LookupUsernameLinkRequest.newBuilder().build()),

        // Bad handle length
        Arguments.of(LookupUsernameLinkRequest.newBuilder()
            .setUsernameLinkHandle(ByteString.copyFrom(new byte[15]))
            .build())
    );
  }

  @Test
  void lookupUsernameLinkRateLimited() {
    final Duration retryAfter = Duration.ofSeconds(17);

    when(rateLimiter.validateReactive(anyString()))
        .thenReturn(Mono.error(new RateLimitExceededException(retryAfter)));

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertRateLimitExceeded(retryAfter,
        () -> unauthenticatedServiceStub().lookupUsernameLink(LookupUsernameLinkRequest.newBuilder()
            .setUsernameLinkHandle(UUIDUtil.toByteString(UUID.randomUUID()))
            .build()),
        accountsManager);
  }
}
