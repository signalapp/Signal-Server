/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.core.Response;
import org.glassfish.jersey.server.ServerProperties;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.signal.libsignal.usernames.BaseUsernameException;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.auth.StoredRegistrationLock;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.AccountIdentifierResponse;
import org.whispersystems.textsecuregcm.entities.AccountIdentityResponse;
import org.whispersystems.textsecuregcm.entities.ApnRegistrationId;
import org.whispersystems.textsecuregcm.entities.ConfirmUsernameHashRequest;
import org.whispersystems.textsecuregcm.entities.EncryptedUsername;
import org.whispersystems.textsecuregcm.entities.GcmRegistrationId;
import org.whispersystems.textsecuregcm.entities.RegistrationLock;
import org.whispersystems.textsecuregcm.entities.ReserveUsernameHashRequest;
import org.whispersystems.textsecuregcm.entities.ReserveUsernameHashResponse;
import org.whispersystems.textsecuregcm.entities.UsernameHashResponse;
import org.whispersystems.textsecuregcm.entities.UsernameLinkHandle;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimitByIpFilter;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.mappers.ImpossiblePhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.JsonMappingExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.NonNormalizedPhoneNumberExceptionMapper;
import org.whispersystems.textsecuregcm.mappers.RateLimitExceededExceptionMapper;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;
import org.whispersystems.textsecuregcm.storage.UsernameHashNotAvailableException;
import org.whispersystems.textsecuregcm.storage.UsernameReservationNotFoundException;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.TestRemoteAddressFilterProvider;
import org.whispersystems.textsecuregcm.util.UsernameHashZkProofVerifier;

@ExtendWith(DropwizardExtensionsSupport.class)
class AccountControllerTest {
  private static final String SENDER             = "+14152222222";
  private static final String SENDER_OLD         = "+14151111111";
  private static final String SENDER_PIN         = "+14153333333";
  private static final String SENDER_OVER_PIN    = "+14154444444";
  private static final String SENDER_PREAUTH     = "+14157777777";
  private static final String SENDER_REG_LOCK    = "+14158888888";
  private static final String SENDER_HAS_STORAGE = "+14159999999";
  private static final String SENDER_TRANSFER    = "+14151111112";
  private static final String BASE_64_URL_USERNAME_HASH_1 = "9p6Tip7BFefFOJzv4kv4GyXEYsBVfk_WbjNejdlOvQE";
  private static final String BASE_64_URL_USERNAME_HASH_2 = "NLUom-CHwtemcdvOTTXdmXmzRIV7F05leS8lwkVK_vc";
  private static final String BASE_64_URL_ENCRYPTED_USERNAME_1 = "md1votbj9r794DsqTNrBqA";

  private static final String INVALID_BASE_64_URL_USERNAME_HASH = "fA+VkNbvB6dVfx/6NpaRSK6mvhhAUBgDNWFaD7+7gvs=";
  private static final String TOO_SHORT_BASE_64_URL_USERNAME_HASH = "P2oMuxx0xgGxSpTO0ACq3IztEOBDaV9t9YFu4bAGpQ";
  private static final byte[] USERNAME_HASH_1 = Base64.getUrlDecoder().decode(BASE_64_URL_USERNAME_HASH_1);
  private static final byte[] USERNAME_HASH_2 = Base64.getUrlDecoder().decode(BASE_64_URL_USERNAME_HASH_2);
  private static final byte[] ENCRYPTED_USERNAME_1 = Base64.getUrlDecoder().decode(BASE_64_URL_ENCRYPTED_USERNAME_1);
  private static final byte[] INVALID_USERNAME_HASH = Base64.getDecoder().decode(INVALID_BASE_64_URL_USERNAME_HASH);
  private static final byte[] TOO_SHORT_USERNAME_HASH = Base64.getUrlDecoder().decode(TOO_SHORT_BASE_64_URL_USERNAME_HASH);
  private static final String BASE_64_URL_ZK_PROOF = "2kambOgmdeeIO0faCMgR6HR4G2BQ5bnhXdIe9ZuZY0NmQXSra5BzDBQ7jzy1cvoEqUHYLpBYMrXudkYPJaWoQg";
  private static final byte[] ZK_PROOF = Base64.getUrlDecoder().decode(BASE_64_URL_ZK_PROOF);
  private static final UUID   SENDER_REG_LOCK_UUID = UUID.randomUUID();
  private static final UUID   SENDER_TRANSFER_UUID = UUID.randomUUID();

  private static AccountsManager accountsManager = mock(AccountsManager.class);
  private static RateLimiters rateLimiters = mock(RateLimiters.class);
  private static RateLimiter rateLimiter = mock(RateLimiter.class);
  private static RateLimiter usernameSetLimiter = mock(RateLimiter.class);
  private static RateLimiter usernameReserveLimiter = mock(RateLimiter.class);
  private static RateLimiter usernameLookupLimiter = mock(RateLimiter.class);
  private static RateLimiter checkAccountExistence = mock(RateLimiter.class);
  private static TurnTokenGenerator turnTokenGenerator = mock(TurnTokenGenerator.class);
  private static Account senderPinAccount = mock(Account.class);
  private static Account senderRegLockAccount = mock(Account.class);
  private static Account senderHasStorage = mock(Account.class);
  private static Account senderTransfer = mock(Account.class);
  private static RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager = mock(
      RegistrationRecoveryPasswordsManager.class);
  private static final UsernameHashZkProofVerifier usernameZkProofVerifier = mock(UsernameHashZkProofVerifier.class);

  private byte[] registration_lock_key = new byte[32];

  private static final TestRemoteAddressFilterProvider TEST_REMOTE_ADDRESS_FILTER_PROVIDER
      = new TestRemoteAddressFilterProvider("127.0.0.1");

  private static final ResourceExtension resources = ResourceExtension.builder()
      .addProperty(ServerProperties.UNWRAP_COMPLETION_STAGE_IN_WRITER_ENABLE, Boolean.TRUE)
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedAccount.class))
      .addProvider(new JsonMappingExceptionMapper())
      .addProvider(new RateLimitExceededExceptionMapper())
      .addProvider(new ImpossiblePhoneNumberExceptionMapper())
      .addProvider(new NonNormalizedPhoneNumberExceptionMapper())
      .addProvider(TEST_REMOTE_ADDRESS_FILTER_PROVIDER)
      .addProvider(new RateLimitByIpFilter(rateLimiters))
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(new AccountController(
              accountsManager,
          rateLimiters,
          turnTokenGenerator,
          registrationRecoveryPasswordsManager,
          usernameZkProofVerifier
      ))
      .build();


  @BeforeEach
  void setup() throws Exception {
    clearInvocations(AuthHelper.VALID_ACCOUNT, AuthHelper.UNDISCOVERABLE_ACCOUNT);

    new SecureRandom().nextBytes(registration_lock_key);
    SaltedTokenHash registrationLockCredentials = SaltedTokenHash.generateFor(
        HexFormat.of().formatHex(registration_lock_key));

    AccountsHelper.setupMockUpdate(accountsManager);

    when(rateLimiters.getUsernameSetLimiter()).thenReturn(usernameSetLimiter);
    when(rateLimiters.getUsernameReserveLimiter()).thenReturn(usernameReserveLimiter);
    when(rateLimiters.getUsernameLookupLimiter()).thenReturn(usernameLookupLimiter);
    when(rateLimiters.forDescriptor(eq(RateLimiters.For.USERNAME_LOOKUP))).thenReturn(usernameLookupLimiter);
    when(rateLimiters.forDescriptor(eq(RateLimiters.For.CHECK_ACCOUNT_EXISTENCE))).thenReturn(checkAccountExistence);

    when(usernameSetLimiter.validateAsync(any(UUID.class))).thenReturn(CompletableFuture.completedFuture(null));

    when(senderPinAccount.getLastSeen()).thenReturn(System.currentTimeMillis());
    when(senderPinAccount.getRegistrationLock()).thenReturn(
        new StoredRegistrationLock(Optional.empty(), Optional.empty(), Instant.ofEpochMilli(System.currentTimeMillis())));

    when(senderHasStorage.getUuid()).thenReturn(UUID.randomUUID());
    when(senderHasStorage.isStorageSupported()).thenReturn(true);
    when(senderHasStorage.getRegistrationLock()).thenReturn(
        new StoredRegistrationLock(Optional.empty(), Optional.empty(), Instant.ofEpochMilli(System.currentTimeMillis())));

    when(senderRegLockAccount.getRegistrationLock()).thenReturn(
        new StoredRegistrationLock(Optional.of(registrationLockCredentials.hash()),
            Optional.of(registrationLockCredentials.salt()), Instant.ofEpochMilli(System.currentTimeMillis())));
    when(senderRegLockAccount.getLastSeen()).thenReturn(System.currentTimeMillis());
    when(senderRegLockAccount.getUuid()).thenReturn(SENDER_REG_LOCK_UUID);
    when(senderRegLockAccount.getNumber()).thenReturn(SENDER_REG_LOCK);

    when(senderTransfer.getRegistrationLock()).thenReturn(
        new StoredRegistrationLock(Optional.empty(), Optional.empty(), Instant.ofEpochMilli(System.currentTimeMillis())));
    when(senderTransfer.getUuid()).thenReturn(SENDER_TRANSFER_UUID);
    when(senderTransfer.getNumber()).thenReturn(SENDER_TRANSFER);

    when(accountsManager.getByE164(eq(SENDER_PIN))).thenReturn(Optional.of(senderPinAccount));
    when(accountsManager.getByE164(eq(SENDER_REG_LOCK))).thenReturn(Optional.of(senderRegLockAccount));
    when(accountsManager.getByE164(eq(SENDER_OVER_PIN))).thenReturn(Optional.of(senderPinAccount));
    when(accountsManager.getByE164(eq(SENDER))).thenReturn(Optional.empty());
    when(accountsManager.getByE164(eq(SENDER_OLD))).thenReturn(Optional.empty());
    when(accountsManager.getByE164(eq(SENDER_PREAUTH))).thenReturn(Optional.empty());
    when(accountsManager.getByE164(eq(SENDER_HAS_STORAGE))).thenReturn(Optional.of(senderHasStorage));
    when(accountsManager.getByE164(eq(SENDER_TRANSFER))).thenReturn(Optional.of(senderTransfer));

    doAnswer(invocation -> {
      final byte[] proof = invocation.getArgument(0);
      final byte[] hash = invocation.getArgument(1);

      if (proof == null || hash == null) {
        throw new NullPointerException();
      }

      return null;
    }).when(usernameZkProofVerifier).verifyProof(any(), any());
  }

  @AfterEach
  void teardown() {
    reset(
        accountsManager,
        rateLimiters,
        rateLimiter,
        usernameSetLimiter,
        usernameReserveLimiter,
        usernameLookupLimiter,
        turnTokenGenerator,
        senderPinAccount,
        senderRegLockAccount,
        senderHasStorage,
        senderTransfer,
        usernameZkProofVerifier);

    clearInvocations(AuthHelper.VALID_DEVICE_3_PRIMARY);
  }

  @Test
  void testSetRegistrationLock() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/registration_lock/")
                 .request()
                 .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.json(new RegistrationLock("1234567890123456789012345678901234567890123456789012345678901234")));

    assertThat(response.getStatus()).isEqualTo(204);

    ArgumentCaptor<String> pinCapture     = ArgumentCaptor.forClass(String.class);
    ArgumentCaptor<String> pinSaltCapture = ArgumentCaptor.forClass(String.class);

    verify(AuthHelper.VALID_ACCOUNT, times(1)).setRegistrationLock(pinCapture.capture(), pinSaltCapture.capture());

    assertThat(pinCapture.getValue()).isNotEmpty();
    assertThat(pinSaltCapture.getValue()).isNotEmpty();

    assertThat(pinCapture.getValue().length()).isEqualTo(66);
  }

  @Test
  void testSetShortRegistrationLock() throws Exception {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/registration_lock/")
                 .request()
                 .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .put(Entity.json(new RegistrationLock("313")));

    assertThat(response.getStatus()).isEqualTo(422);
  }

  @Test
  void testSetGcmId() {
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/gcm/")
            .request()
            .header(HttpHeaders.AUTHORIZATION,
                AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_3, AuthHelper.VALID_PASSWORD_3_PRIMARY))
            .put(Entity.json(new GcmRegistrationId("z000")));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(AuthHelper.VALID_DEVICE_3_PRIMARY, times(1)).setGcmId(eq("z000"));
    verify(accountsManager, times(1)).updateDevice(eq(AuthHelper.VALID_ACCOUNT_3), anyByte(), any());
  }

  @Test
  void testSetGcmIdInvalidrequest() {
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/gcm/")
            .request()
            .header(HttpHeaders.AUTHORIZATION,
                AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_3, AuthHelper.VALID_PASSWORD_3_PRIMARY))
            .put(Entity.json("{}"));

    assertThat(response.getStatus()).isEqualTo(422);

  }

  @Test
  void testSetApnId() {
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/apn/")
            .request()
            .header(HttpHeaders.AUTHORIZATION,
                AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_3, AuthHelper.VALID_PASSWORD_3_PRIMARY))
            .put(Entity.json(new ApnRegistrationId("first", "second")));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(AuthHelper.VALID_DEVICE_3_PRIMARY, times(1)).setApnId(eq("first"));
    verify(AuthHelper.VALID_DEVICE_3_PRIMARY, times(1)).setVoipApnId(eq("second"));
    verify(accountsManager, times(1)).updateDevice(eq(AuthHelper.VALID_ACCOUNT_3), anyByte(), any());
  }

  @Test
  void testSetApnIdNoVoip() {
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/apn/")
            .request()
            .header(HttpHeaders.AUTHORIZATION,
                AuthHelper.getAuthHeader(AuthHelper.VALID_UUID_3, AuthHelper.VALID_PASSWORD_3_PRIMARY))
            .put(Entity.json(new ApnRegistrationId("first", null)));

    assertThat(response.getStatus()).isEqualTo(204);

    verify(AuthHelper.VALID_DEVICE_3_PRIMARY, times(1)).setApnId(eq("first"));
    verify(AuthHelper.VALID_DEVICE_3_PRIMARY, times(1)).setVoipApnId(null);
    verify(accountsManager, times(1)).updateDevice(eq(AuthHelper.VALID_ACCOUNT_3), anyByte(), any());
  }

  @ParameterizedTest
  @ValueSource(strings = {"/v1/accounts/whoami", "/v1/accounts/me"})
  void testWhoAmI(final String path) {


    final Response response = resources.getJerseyTest()
        .target(path)
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get();

    assertThat(response.getStatus()).isEqualTo(200);

    assertThat(response.readEntity(AccountIdentityResponse.class).uuid()).isEqualTo(AuthHelper.VALID_UUID);
  }

  static Stream<Arguments> testSetUsernameLink() {
    return Stream.of(
        Arguments.of(false, true, true, 32, 401),
        Arguments.of(true, true, false, 32, 409),
        Arguments.of(true, true, true, 129, 422),
        Arguments.of(true, true, true, 0, 422),
        Arguments.of(true, false, true, 32, 429),
        Arguments.of(true, true, true, 128, 200)
    );
  }

  @ParameterizedTest
  @MethodSource
  public void testSetUsernameLink(
      final boolean auth,
      final boolean passRateLimiting,
      final boolean setUsernameHash,
      final int payloadSize,
      final int expectedStatus) throws Exception {

    // checking if rate limiting needs to pass or fail for this test
    if (passRateLimiting) {
      MockUtils.updateRateLimiterResponseToAllow(
          rateLimiters, RateLimiters.For.USERNAME_LINK_OPERATION, AuthHelper.VALID_UUID);
    } else {
      MockUtils.updateRateLimiterResponseToFail(
          rateLimiters, RateLimiters.For.USERNAME_LINK_OPERATION, AuthHelper.VALID_UUID, Duration.ofMinutes(10), false);
    }

    // checking if username is to be set for this test
    if (setUsernameHash) {
      when(AuthHelper.VALID_ACCOUNT.getUsernameHash()).thenReturn(Optional.of(USERNAME_HASH_1));
    } else {
      when(AuthHelper.VALID_ACCOUNT.getUsernameHash()).thenReturn(Optional.empty());
    }

    final Invocation.Builder builder = resources.getJerseyTest()
        .target("/v1/accounts/username_link")
        .request();

    // checking if auth is needed for this test
    if (auth) {
      builder.header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
    }

    // make sure `update()` works
    doReturn(AuthHelper.VALID_ACCOUNT).when(accountsManager).update(any(), any());

    final Response put = builder.put(Entity.json(new EncryptedUsername(TestRandomUtil.nextBytes(payloadSize))));

    assertEquals(expectedStatus, put.getStatus());
  }

  static Stream<Arguments> testDeleteUsernameLink() {
    return Stream.of(
        Arguments.of(false, true, 401),
        Arguments.of(true, false, 429),
        Arguments.of(true, true, 204)
    );
  }

  @ParameterizedTest
  @MethodSource
  public void testDeleteUsernameLink(
      final boolean auth,
      final boolean passRateLimiting,
      final int expectedStatus) throws Exception {

    // checking if rate limiting needs to pass or fail for this test
    if (passRateLimiting) {
      MockUtils.updateRateLimiterResponseToAllow(
          rateLimiters, RateLimiters.For.USERNAME_LINK_OPERATION, AuthHelper.VALID_UUID);
    } else {
      MockUtils.updateRateLimiterResponseToFail(
          rateLimiters, RateLimiters.For.USERNAME_LINK_OPERATION, AuthHelper.VALID_UUID, Duration.ofMinutes(10), false);
    }

    final Invocation.Builder builder = resources.getJerseyTest()
        .target("/v1/accounts/username_link")
        .request();

    // checking if auth is needed for this test
    if (auth) {
      builder.header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
    }

    // make sure `update()` works
    doReturn(AuthHelper.VALID_ACCOUNT).when(accountsManager).update(any(), any());

    final Response delete = builder.delete();

    assertEquals(expectedStatus, delete.getStatus());
  }

  static Stream<Arguments> testLookupUsernameLink() {
    return Stream.of(
        Arguments.of(false, true, true, true, 400),
        Arguments.of(true, false, true, true, 429),
        Arguments.of(true, true, false, true, 404),
        Arguments.of(true, true, true, false, 404),
        Arguments.of(true, true, true, true, 200)
    );
  }

  @ParameterizedTest
  @MethodSource
  public void testLookupUsernameLink(
      final boolean stayUnauthenticated,
      final boolean passRateLimiting,
      final boolean validUuidInput,
      final boolean locateLinkByUuid,
      final int expectedStatus) {

    if (passRateLimiting) {
      MockUtils.updateRateLimiterResponseToAllow(
          rateLimiters, RateLimiters.For.USERNAME_LINK_LOOKUP_PER_IP, "127.0.0.1");
    } else {
      MockUtils.updateRateLimiterResponseToFail(
          rateLimiters, RateLimiters.For.USERNAME_LINK_LOOKUP_PER_IP, "127.0.0.1", Duration.ofMinutes(10), false);
    }

    when(accountsManager.getByUsernameLinkHandle(any())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    final String uuid = validUuidInput ? UUID.randomUUID().toString() : "invalid-uuid";

    if (validUuidInput && locateLinkByUuid) {
      final Account account = mock(Account.class);
      when(account.getEncryptedUsername()).thenReturn(Optional.of(TestRandomUtil.nextBytes(16)));
      when(accountsManager.getByUsernameLinkHandle(UUID.fromString(uuid))).thenReturn(CompletableFuture.completedFuture(Optional.of(account)));
    }

    final Invocation.Builder builder = resources.getJerseyTest()
        .target("/v1/accounts/username_link/" + uuid)
        .request();
    if (!stayUnauthenticated) {
      builder.header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD));
    }
    final Response get = builder.get();

    assertEquals(expectedStatus, get.getStatus());
  }

  @Test
  void testReserveUsernameHash() {
    when(accountsManager.reserveUsernameHash(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(new AccountsManager.UsernameReservation(null, USERNAME_HASH_1)));
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username_hash/reserve")
            .request()
            .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new ReserveUsernameHashRequest(List.of(USERNAME_HASH_1, USERNAME_HASH_2))));
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.readEntity(ReserveUsernameHashResponse.class))
        .satisfies(r -> assertThat(r.usernameHash()).hasSize(32));
  }

  @Test
  void testReserveUsernameHashUnavailable() {
    when(accountsManager.reserveUsernameHash(any(), anyList()))
        .thenReturn(CompletableFuture.failedFuture(new UsernameHashNotAvailableException()));
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username_hash/reserve")
            .request()
            .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new ReserveUsernameHashRequest(List.of(USERNAME_HASH_1, USERNAME_HASH_2))));
    assertThat(response.getStatus()).isEqualTo(409);
  }

  @ParameterizedTest
  @MethodSource
  void testReserveUsernameHashListSizeInvalid(List<byte[]> usernameHashes) {
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username_hash/reserve")
            .request()
            .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new ReserveUsernameHashRequest(usernameHashes)));
    assertThat(response.getStatus()).isEqualTo(422);
  }

  static Stream<Arguments> testReserveUsernameHashListSizeInvalid() {
    return Stream.of(
        Arguments.of(Collections.nCopies(21, USERNAME_HASH_1)),
        Arguments.of(Collections.emptyList())
    );
  }

  @Test
  void testReserveUsernameHashInvalidHashSize() {
    List<byte[]> usernameHashes = List.of(new byte[31]);
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username_hash/reserve")
            .request()
            .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new ReserveUsernameHashRequest(usernameHashes)));
    assertThat(response.getStatus()).isEqualTo(422);
  }

  @Test
  void testReserveUsernameHashNullList() {
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username_hash/reserve")
            .request()
            .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new ReserveUsernameHashRequest(null)));
    assertThat(response.getStatus()).isEqualTo(422);
  }

  @Test
  void testReserveUsernameHashInvalidBase64UrlEncoding() {
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username_hash/reserve")
            .request()
            .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(
                // Has '+' and '='characters which are invalid in base64url
                """
                  {
                    "usernameHashes": ["jh1jJ50oGn9wUXAFNtDus6AJgWOQ6XbZzF+wCv7OOQs="]
                  }
                """));
    assertThat(response.getStatus()).isEqualTo(422);
  }

  @Test
  void testConfirmUsernameHash() throws BaseUsernameException {
    Account account = mock(Account.class);
    final UUID uuid = UUID.randomUUID();
    when(account.getUsernameHash()).thenReturn(Optional.of(USERNAME_HASH_1));
    when(account.getUsernameLinkHandle()).thenReturn(uuid);
    when(accountsManager.confirmReservedUsernameHash(any(), eq(USERNAME_HASH_1), eq(ENCRYPTED_USERNAME_1)))
        .thenReturn(CompletableFuture.completedFuture(account));

    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username_hash/confirm")
            .request()
            .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new ConfirmUsernameHashRequest(USERNAME_HASH_1, ZK_PROOF, ENCRYPTED_USERNAME_1)));
    assertThat(response.getStatus()).isEqualTo(200);

    final UsernameHashResponse respEntity = response.readEntity(UsernameHashResponse.class);
    assertArrayEquals(respEntity.usernameHash(), USERNAME_HASH_1);
    assertEquals(respEntity.usernameLinkHandle(), uuid);
    verify(usernameZkProofVerifier).verifyProof(ZK_PROOF, USERNAME_HASH_1);
  }

  @Test
  void testConfirmUsernameHashNullProof() {
    try (final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username_hash/confirm")
            .request()
            .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new ConfirmUsernameHashRequest(USERNAME_HASH_1, null, ENCRYPTED_USERNAME_1)))) {

      assertThat(response.getStatus()).isEqualTo(422);
    }
  }

  @Test
  void testConfirmUsernameHashOld() throws BaseUsernameException {
    Account account = mock(Account.class);
    when(account.getUsernameHash()).thenReturn(Optional.of(USERNAME_HASH_1));
    when(account.getUsernameLinkHandle()).thenReturn(null);
    when(accountsManager.confirmReservedUsernameHash(any(), eq(USERNAME_HASH_1), eq(null)))
        .thenReturn(CompletableFuture.completedFuture(account));



    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username_hash/confirm")
            .request()
            .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new ConfirmUsernameHashRequest(USERNAME_HASH_1, ZK_PROOF, null)));
    assertThat(response.getStatus()).isEqualTo(200);

    final UsernameHashResponse respEntity = response.readEntity(UsernameHashResponse.class);
    assertArrayEquals(respEntity.usernameHash(), USERNAME_HASH_1);
    assertNull(respEntity.usernameLinkHandle());
    verify(usernameZkProofVerifier).verifyProof(ZK_PROOF, USERNAME_HASH_1);
  }

  @Test
  void testConfirmUnreservedUsernameHash() throws BaseUsernameException {
    when(accountsManager.confirmReservedUsernameHash(any(), eq(USERNAME_HASH_1), any()))
        .thenReturn(CompletableFuture.failedFuture(new UsernameReservationNotFoundException()));

    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username_hash/confirm")
            .request()
            .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new ConfirmUsernameHashRequest(USERNAME_HASH_1, ZK_PROOF, ENCRYPTED_USERNAME_1)));
    assertThat(response.getStatus()).isEqualTo(409);
    verify(usernameZkProofVerifier).verifyProof(ZK_PROOF, USERNAME_HASH_1);
  }

  @Test
  void testConfirmLapsedUsernameHash() throws BaseUsernameException {
    when(accountsManager.confirmReservedUsernameHash(any(), eq(USERNAME_HASH_1), any()))
        .thenReturn(CompletableFuture.failedFuture(new UsernameHashNotAvailableException()));

    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username_hash/confirm")
            .request()
            .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new ConfirmUsernameHashRequest(USERNAME_HASH_1, ZK_PROOF, ENCRYPTED_USERNAME_1)));
    assertThat(response.getStatus()).isEqualTo(410);
    verify(usernameZkProofVerifier).verifyProof(ZK_PROOF, USERNAME_HASH_1);
  }

  @Test
  void testConfirmUsernameHashInvalidBase64UrlEncoding() {
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username_hash/confirm")
            .request()
            .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(
                // Has '+' and '='characters which are invalid in base64url
                """
                  {
                    "usernameHash": "jh1jJ50oGn9wUXAFNtDus6AJgWOQ6XbZzF+wCv7OOQs=",
                    "zkProof": "iYXE0QPK60PS3lGa-xdNv0GlXA3B03xQLzltSf-2xmscyS_8fjy5H9ymfaEr62PcVY7tsWhWjOOvcCnhmP_HS="
                  }
                """));
    assertThat(response.getStatus()).isEqualTo(422);
    verifyNoInteractions(usernameZkProofVerifier);
  }

  @Test
  void testConfirmUsernameHashInvalidHashSize() {
    byte[] usernameHash = new byte[31];
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username_hash/confirm")
            .request()
            .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new ConfirmUsernameHashRequest(usernameHash, ZK_PROOF, ENCRYPTED_USERNAME_1)));
    assertThat(response.getStatus()).isEqualTo(422);
    verifyNoInteractions(usernameZkProofVerifier);
  }

  @Test
  void testCommitUsernameHashWithInvalidProof() throws BaseUsernameException {
    doThrow(new BaseUsernameException("invalid username")).when(usernameZkProofVerifier).verifyProof(eq(ZK_PROOF), eq(USERNAME_HASH_1));
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/username_hash/confirm")
            .request()
            .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new ConfirmUsernameHashRequest(USERNAME_HASH_1, ZK_PROOF, ENCRYPTED_USERNAME_1)));
    assertThat(response.getStatus()).isEqualTo(422);
    verify(usernameZkProofVerifier).verifyProof(ZK_PROOF, USERNAME_HASH_1);
  }

  @Test
  void testDeleteUsername() {
    when(accountsManager.clearUsernameHash(any()))
        .thenAnswer(invocation -> CompletableFutureTestUtil.almostCompletedFuture(invocation.getArgument(0)));

    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username_hash/")
                 .request()
                 .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                 .delete();

    assertThat(response.readEntity(String.class)).isEqualTo("");
    assertThat(response.getStatus()).isEqualTo(204);
    verify(accountsManager).clearUsernameHash(AuthHelper.VALID_ACCOUNT);
  }

  @Test
  void testDeleteUsernameBadAuth() {
    Response response =
        resources.getJerseyTest()
                 .target("/v1/accounts/username_hash/")
                 .request()
                 .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.INVALID_PASSWORD))
                 .delete();

    assertThat(response.getStatus()).isEqualTo(401);
  }

  @Test
  void testSetAccountAttributesNoDiscoverabilityChange() {
    Response response =
            resources.getJerseyTest()
                    .target("/v1/accounts/attributes/")
                    .request()
                    .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                    .put(Entity.json(new AccountAttributes(false, 2222, 3333, null, null, true, null)));

    assertThat(response.getStatus()).isEqualTo(204);
  }

  @Test
  void testSetAccountAttributesEnableDiscovery() {
    Response response =
            resources.getJerseyTest()
                    .target("/v1/accounts/attributes/")
                    .request()
                    .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.UNDISCOVERABLE_UUID, AuthHelper.UNDISCOVERABLE_PASSWORD))
                    .put(Entity.json(new AccountAttributes(false, 2222, 3333, null, null, true, null)));

    assertThat(response.getStatus()).isEqualTo(204);
  }

  @Test
  void testAccountsAttributesUpdateRecoveryPassword() {
    final byte[] recoveryPassword = TestRandomUtil.nextBytes(32);
    final Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/attributes/")
            .request()
            .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.UNDISCOVERABLE_UUID, AuthHelper.UNDISCOVERABLE_PASSWORD))
            .put(Entity.json(new AccountAttributes(false, 2222, 3333, null, null, true, null)
                .withRecoveryPassword(recoveryPassword)));

    assertThat(response.getStatus()).isEqualTo(204);
    verify(registrationRecoveryPasswordsManager).storeForCurrentNumber(eq(AuthHelper.UNDISCOVERABLE_NUMBER), eq(recoveryPassword));
  }

  @Test
  void testSetAccountAttributesDisableDiscovery() {
    Response response =
            resources.getJerseyTest()
                    .target("/v1/accounts/attributes/")
                    .request()
                    .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                    .put(Entity.json(new AccountAttributes(false, 2222, 3333, null, null, false, null)));

    assertThat(response.getStatus()).isEqualTo(204);
  }

  @Test
  void testSetAccountAttributesBadUnidentifiedKeyLength() {
    Response response =
        resources.getJerseyTest()
            .target("/v1/accounts/attributes/")
            .request()
            .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
            .put(Entity.json(new AccountAttributes(false, 2222, 3333, null, null, false, null)
                .withUnidentifiedAccessKey(new byte[7])));

    assertThat(response.getStatus()).isEqualTo(422);
  }

  @Test
  void testDeleteAccount() {
    when(accountsManager.delete(any(), any())).thenReturn(CompletableFutureTestUtil.almostCompletedFuture(null));

    Response response =
            resources.getJerseyTest()
                     .target("/v1/accounts/me")
                     .request()
                     .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
                     .delete();

    assertThat(response.getStatus()).isEqualTo(204);
    verify(accountsManager).delete(AuthHelper.VALID_ACCOUNT, AccountsManager.DeletionReason.USER_REQUEST);
  }

  @Test
  void testDeleteAccountException() {
    when(accountsManager.delete(any(), any())).thenReturn(CompletableFuture.failedFuture(new RuntimeException("OH NO")));

    try (final Response response = resources.getJerseyTest()
        .target("/v1/accounts/me")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .delete()) {

      assertThat(response.getStatus()).isEqualTo(500);
      verify(accountsManager).delete(AuthHelper.VALID_ACCOUNT, AccountsManager.DeletionReason.USER_REQUEST);
    }
  }

  @Test
  void testAccountExists() {
    final Account account = mock(Account.class);

    final UUID accountIdentifier = UUID.randomUUID();
    final UUID phoneNumberIdentifier = UUID.randomUUID();

    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.empty());
    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(accountIdentifier))).thenReturn(Optional.of(account));
    when(accountsManager.getByServiceIdentifier(new PniServiceIdentifier(phoneNumberIdentifier))).thenReturn(Optional.of(account));

    when(rateLimiters.getCheckAccountExistenceLimiter()).thenReturn(mock(RateLimiter.class));

    assertThat(resources.getJerseyTest()
        .target(String.format("/v1/accounts/account/%s", accountIdentifier))
        .request()
        .head()
        .getStatus()).isEqualTo(200);

    assertThat(resources.getJerseyTest()
        .target(String.format("/v1/accounts/account/PNI:%s", phoneNumberIdentifier))
        .request()
        .head()
        .getStatus()).isEqualTo(200);

    assertThat(resources.getJerseyTest()
        .target(String.format("/v1/accounts/account/%s", UUID.randomUUID()))
        .request()
        .head()
        .getStatus()).isEqualTo(404);
  }

  @Test
  void testAccountExistsRateLimited() throws RateLimitExceededException {
    final Duration expectedRetryAfter = Duration.ofSeconds(13);
    final Account account = mock(Account.class);
    final UUID accountIdentifier = UUID.randomUUID();
    when(accountsManager.getByAccountIdentifier(accountIdentifier)).thenReturn(Optional.of(account));

    MockUtils.updateRateLimiterResponseToFail(
        rateLimiters, RateLimiters.For.CHECK_ACCOUNT_EXISTENCE, "127.0.0.1", expectedRetryAfter, true);

    final Response response = resources.getJerseyTest()
        .target(String.format("/v1/accounts/account/%s", accountIdentifier))
        .request()
        .head();

    assertThat(response.getStatus()).isEqualTo(413);
    assertThat(response.getHeaderString("Retry-After")).isEqualTo(String.valueOf(expectedRetryAfter.toSeconds()));
  }

  @Test
  void testAccountExistsAuthenticated() {
    assertThat(resources.getJerseyTest()
        .target(String.format("/v1/accounts/account/%s", UUID.randomUUID()))
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .head()
        .getStatus()).isEqualTo(400);
  }

  @Test
  void testLookupUsername() {
    final Account account = mock(Account.class);
    final UUID uuid = UUID.randomUUID();
    when(account.getUuid()).thenReturn(uuid);

    when(accountsManager.getByUsernameHash(any())).thenReturn(CompletableFuture.completedFuture(Optional.of(account)));
    Response response = resources.getJerseyTest()
        .target(String.format("v1/accounts/username_hash/%s", BASE_64_URL_USERNAME_HASH_1))
        .request()
        .get();
    assertThat(response.getStatus()).isEqualTo(200);
    assertThat(response.readEntity(AccountIdentifierResponse.class).uuid().uuid()).isEqualTo(uuid);
  }

  @Test
  void testLookupUsernameDoesNotExist() {
    when(accountsManager.getByUsernameHash(any())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    assertThat(resources.getJerseyTest()
        .target(String.format("v1/accounts/username_hash/%s", BASE_64_URL_USERNAME_HASH_1))
        .request()
        .get().getStatus()).isEqualTo(404);
  }

  @Test
  void testLookupUsernameRateLimited() throws RateLimitExceededException {
    final Duration expectedRetryAfter = Duration.ofSeconds(13);
    MockUtils.updateRateLimiterResponseToFail(
        rateLimiters, RateLimiters.For.USERNAME_LOOKUP, "127.0.0.1", expectedRetryAfter, true);
    final Response response = resources.getJerseyTest()
        .target(String.format("v1/accounts/username_hash/%s", BASE_64_URL_USERNAME_HASH_1))
        .request()
        .get();

    assertThat(response.getStatus()).isEqualTo(413);
    assertThat(response.getHeaderString("Retry-After")).isEqualTo(String.valueOf(expectedRetryAfter.toSeconds()));
  }

  @Test
  void testLookupUsernameAuthenticated() {
    assertThat(resources.getJerseyTest()
        .target(String.format("/v1/accounts/username_hash/%s", USERNAME_HASH_1))
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .get()
        .getStatus()).isEqualTo(400);
  }

  @Test
  void testLookupUsernameInvalidFormat() {
    assertThat(resources.getJerseyTest()
        .target(String.format("/v1/accounts/username_hash/%s", INVALID_USERNAME_HASH))
        .request()
        .get()
        .getStatus()).isEqualTo(422);

    assertThat(resources.getJerseyTest()
        .target(String.format("/v1/accounts/username_hash/%s", TOO_SHORT_USERNAME_HASH))
        .request()
        .get()
        .getStatus()).isEqualTo(422);
  }

  @ParameterizedTest
  @ValueSource(booleans = { true, false })
  void testPutUsernameLink(boolean keepLink) {
    when(rateLimiters.forDescriptor(eq(RateLimiters.For.USERNAME_LINK_OPERATION))).thenReturn(mock(RateLimiter.class));

    final UUID oldLinkHandle = UUID.randomUUID();
    when(AuthHelper.VALID_ACCOUNT.getUsernameLinkHandle()).thenReturn(oldLinkHandle);

    final byte[] encryptedUsername = "some encrypted goop".getBytes();
    final UsernameLinkHandle newHandle = resources.getJerseyTest()
        .target("/v1/accounts/username_link")
        .request()
        .header(HttpHeaders.AUTHORIZATION, AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.json(new EncryptedUsername(encryptedUsername, keepLink)), UsernameLinkHandle.class);

    assertThat(newHandle.usernameLinkHandle().equals(oldLinkHandle)).isEqualTo(keepLink);
    verify(AuthHelper.VALID_ACCOUNT).setUsernameLinkDetails(eq(newHandle.usernameLinkHandle()), eq(encryptedUsername));
  }

}
