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
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
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
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.signal.chat.account.AccountsAnonymousGrpc;
import org.signal.chat.account.Capabilities;
import org.signal.chat.account.CheckAccountExistenceRequest;
import org.signal.chat.account.GetCapabilitiesAnonymousRequest;
import org.signal.chat.account.GetCapabilitiesAnonymousResponse;
import org.signal.chat.account.LookupUsernameHashRequest;
import org.signal.chat.account.LookupUsernameHashResponse;
import org.signal.chat.account.LookupUsernameLinkRequest;
import org.signal.chat.account.LookupUsernameLinkResponse;
import org.signal.chat.common.IdentityType;
import org.signal.chat.common.ServiceIdentifier;
import org.signal.chat.errors.FailedUnidentifiedAuthorization;
import org.signal.chat.errors.NotFound;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.controllers.AccountController;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
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

  @Mock
  private GroupSendTokenUtil groupSendTokenUtil;

  @Override
  protected AccountsAnonymousGrpcService createServiceBeforeEachTest() {
    when(accountsManager.getByServiceIdentifier(any()))
        .thenReturn(Optional.empty());

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

    return new AccountsAnonymousGrpcService(accountsManager, rateLimiters, groupSendTokenUtil);
  }

  @Test
  void checkAccountExistence() {
    final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

    when(accountsManager.getByServiceIdentifier(serviceIdentifier))
        .thenReturn(Optional.of(mock(Account.class)));

    assertTrue(unauthenticatedServiceStub().checkAccountExistence(CheckAccountExistenceRequest.newBuilder()
        .setServiceIdentifier(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier))
        .build()).getAccountExists());

    assertFalse(unauthenticatedServiceStub().checkAccountExistence(CheckAccountExistenceRequest.newBuilder()
        .setServiceIdentifier(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(UUID.randomUUID())))
        .build()).getAccountExists());
  }

  @ParameterizedTest
  @MethodSource
  void checkAccountExistenceIllegalRequest(final CheckAccountExistenceRequest request) {
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
  void checkAccountExistenceRateLimited() throws RateLimitExceededException {
    final Duration retryAfter = Duration.ofSeconds(11);

    doThrow(new RateLimitExceededException(retryAfter))
        .when(rateLimiter).validate(anyString());

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertRateLimitExceeded(retryAfter,
        () -> unauthenticatedServiceStub().checkAccountExistence(CheckAccountExistenceRequest.newBuilder()
            .setServiceIdentifier(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(UUID.randomUUID())))
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

    assertEquals(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(accountIdentifier)),
        unauthenticatedServiceStub().lookupUsernameHash(LookupUsernameHashRequest.newBuilder()
                .setUsernameHash(ByteString.copyFrom(usernameHash))
                .build())
            .getServiceIdentifier());

    assertEquals(LookupUsernameHashResponse.newBuilder().setNotFound(NotFound.getDefaultInstance()).build(),
        unauthenticatedServiceStub().lookupUsernameHash(LookupUsernameHashRequest.newBuilder()
            .setUsernameHash(ByteString.copyFrom(new byte[AccountController.USERNAME_HASH_LENGTH]))
            .build()));
  }

  @ParameterizedTest
  @MethodSource
  void lookupUsernameHashIllegalHash(final LookupUsernameHashRequest request) {
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
  void lookupUsernameHashRateLimited() throws RateLimitExceededException {
    final Duration retryAfter = Duration.ofSeconds(13);

    doThrow(new RateLimitExceededException(retryAfter))
        .when(rateLimiter).validate(anyString());

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

    final LookupUsernameLinkResponse notFoundResponse = LookupUsernameLinkResponse.newBuilder()
        .setNotFound(NotFound.getDefaultInstance())
        .build();

    assertEquals(notFoundResponse,
        unauthenticatedServiceStub().lookupUsernameLink(LookupUsernameLinkRequest.newBuilder()
            .setUsernameLinkHandle(UUIDUtil.toByteString(linkHandle))
            .build()));
    assertEquals(notFoundResponse,
        unauthenticatedServiceStub().lookupUsernameLink(LookupUsernameLinkRequest.newBuilder()
            .setUsernameLinkHandle(UUIDUtil.toByteString(UUID.randomUUID()))
            .build()));
  }

  @ParameterizedTest
  @MethodSource
  void lookupUsernameLinkIllegalHandle(final LookupUsernameLinkRequest request) {
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
  void lookupUsernameLinkRateLimited() throws RateLimitExceededException {
    final Duration retryAfter = Duration.ofSeconds(17);

    doThrow(new RateLimitExceededException(retryAfter))
        .when(rateLimiter).validate(anyString());

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertRateLimitExceeded(retryAfter,
        () -> unauthenticatedServiceStub().lookupUsernameLink(LookupUsernameLinkRequest.newBuilder()
            .setUsernameLinkHandle(UUIDUtil.toByteString(UUID.randomUUID()))
            .build()),
        accountsManager);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void getCapabilities(final boolean useGroupSendToken) {
    final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

    final Account account = mock(Account.class);
    when(accountsManager.getByServiceIdentifier(serviceIdentifier)).thenReturn(Optional.of(account));

    // should be returned since it's public
    when(account.hasCapability(DeviceCapability.SPARSE_POST_QUANTUM_RATCHET)).thenReturn(true);
    // should not be returned since it's self-only
    when(account.hasCapability(DeviceCapability.USERNAME_CHANGE_SYNC_MESSAGE)).thenReturn(true);

    final GetCapabilitiesAnonymousRequest.Builder builder = GetCapabilitiesAnonymousRequest.newBuilder()
        .setAccountIdentifier(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier));
    if (useGroupSendToken) {
      when(groupSendTokenUtil.checkGroupSendToken(any(), eq(serviceIdentifier))).thenReturn(true);
      builder.setGroupSendToken(ByteString.copyFrom(TestRandomUtil.nextBytes(32)));
    } else {
      final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);
      when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
      builder.setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey));
    }
    final GetCapabilitiesAnonymousResponse response = unauthenticatedServiceStub().getCapabilities(builder.build());

    assertEquals(Capabilities.newBuilder()
            .addCapabilities(org.signal.chat.common.DeviceCapability.DEVICE_CAPABILITY_SPARSE_POST_QUANTUM_RATCHET)
            .build(),
        response.getCapabilities());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void getCapabilitiesUnauthorized(final boolean useGroupSendToken) {
    final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

    final Account account = mock(Account.class);
    when(accountsManager.getByServiceIdentifier(serviceIdentifier)).thenReturn(Optional.of(account));

    final GetCapabilitiesAnonymousRequest.Builder builder = GetCapabilitiesAnonymousRequest.newBuilder()
        .setAccountIdentifier(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier));
    if (useGroupSendToken) {
      when(groupSendTokenUtil.checkGroupSendToken(any(), eq(serviceIdentifier))).thenReturn(false);
      builder.setGroupSendToken(ByteString.copyFrom(TestRandomUtil.nextBytes(32)));
    } else {
      when(account.getUnidentifiedAccessKey())
          .thenReturn(Optional.of(TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH)));
      builder.setUnidentifiedAccessKey(ByteString.copyFrom(TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH)));
    }
    final GetCapabilitiesAnonymousResponse response = unauthenticatedServiceStub().getCapabilities(builder.build());

    assertEquals(FailedUnidentifiedAuthorization.getDefaultInstance(), response.getFailedUnidentifiedAuthorization());
  }

  @Test
  void getCapabilitiesNotFound() {
    final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    final byte[] groupSendToken = TestRandomUtil.nextBytes(32);

    when(accountsManager.getByServiceIdentifier(serviceIdentifier))
        .thenReturn(Optional.empty());
    when(groupSendTokenUtil.checkGroupSendToken(ByteString.copyFrom(groupSendToken), serviceIdentifier))
        .thenReturn(true);

    final GetCapabilitiesAnonymousResponse response =
        unauthenticatedServiceStub().getCapabilities(GetCapabilitiesAnonymousRequest.newBuilder()
            .setAccountIdentifier(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier))
            .setGroupSendToken(ByteString.copyFrom(groupSendToken))
            .build());

    assertEquals(NotFound.getDefaultInstance(), response.getNotFound());
  }

  private static Stream<Arguments> getCapabilitiesIllegalRequest() {
    final ServiceIdentifier aci =
        GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(UUID.randomUUID()));

    return Stream.of(
        Arguments.argumentSet("no aci", GetCapabilitiesAnonymousRequest.newBuilder()
            .setUnidentifiedAccessKey(
                ByteString.copyFrom(new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]))
            .build()),

        Arguments.argumentSet("wrong uak length", GetCapabilitiesAnonymousRequest.newBuilder()
            .setAccountIdentifier(aci)
            .setUnidentifiedAccessKey(ByteString.copyFrom(new byte[15]))
            .build()),

        Arguments.argumentSet("invalid gse", GetCapabilitiesAnonymousRequest.newBuilder()
            .setAccountIdentifier(aci)
            .setGroupSendToken(ByteString.EMPTY)
            .build()),

        Arguments.argumentSet("no auth", GetCapabilitiesAnonymousRequest.newBuilder()
            .setAccountIdentifier(
                GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(UUID.randomUUID())))
            .build())
    );
  }

  @ParameterizedTest
  @MethodSource
  void getCapabilitiesIllegalRequest(final GetCapabilitiesAnonymousRequest request) {
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> unauthenticatedServiceStub().getCapabilities(request));
  }
}
