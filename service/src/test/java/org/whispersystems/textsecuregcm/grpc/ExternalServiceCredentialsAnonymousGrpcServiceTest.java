/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.signal.chat.credentials.AuthCheckResult;
import org.signal.chat.credentials.CheckSvrCredentialsRequest;
import org.signal.chat.credentials.CheckSvrCredentialsResponse;
import org.signal.chat.credentials.ExternalServiceCredentialsAnonymousGrpc;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.MutableClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class ExternalServiceCredentialsAnonymousGrpcServiceTest extends
    SimpleBaseGrpcTest<ExternalServiceCredentialsAnonymousGrpcService, ExternalServiceCredentialsAnonymousGrpc.ExternalServiceCredentialsAnonymousBlockingStub> {

  private static final UUID USER_UUID = UUID.randomUUID();

  private static final String USER_E164 = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("US"),
      PhoneNumberUtil.PhoneNumberFormat.E164
  );

  private static final MutableClock CLOCK = MockUtils.mutableClock(0);

  private static final ExternalServiceCredentialsGenerator SVR_CREDENTIALS_GENERATOR = Mockito.spy(ExternalServiceCredentialsGenerator
      .builder(TestRandomUtil.nextBytes(32))
      .withUserDerivationKey(TestRandomUtil.nextBytes(32))
      .prependUsername(false)
      .withDerivedUsernameTruncateLength(16)
      .withClock(CLOCK)
      .build());

  @Mock
  private AccountsManager accountsManager;

  @Override
  protected ExternalServiceCredentialsAnonymousGrpcService createServiceBeforeEachTest() {
    return new ExternalServiceCredentialsAnonymousGrpcService(accountsManager, SVR_CREDENTIALS_GENERATOR);
  }

  @BeforeEach
  public void setup() {
    Mockito.when(accountsManager.getByE164Async(USER_E164))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account(USER_UUID))));
  }

  @Test
  public void testOneMatch() throws Exception {
    final UUID user2 = UUID.randomUUID();
    final UUID user3 = UUID.randomUUID();
    assertExpectedCredentialCheckResponse(Map.of(
        token(USER_UUID, day(1)), AuthCheckResult.AUTH_CHECK_RESULT_MATCH,
        token(user2, day(1)), AuthCheckResult.AUTH_CHECK_RESULT_NO_MATCH,
        token(user3, day(1)), AuthCheckResult.AUTH_CHECK_RESULT_NO_MATCH
    ), day(2));
  }

  @Test
  public void testNoMatch() throws Exception {
    final UUID user2 = UUID.randomUUID();
    final UUID user3 = UUID.randomUUID();
    assertExpectedCredentialCheckResponse(Map.of(
        token(user2, day(1)), AuthCheckResult.AUTH_CHECK_RESULT_NO_MATCH,
        token(user3, day(1)), AuthCheckResult.AUTH_CHECK_RESULT_NO_MATCH
    ), day(2));
  }

  @Test
  public void testSomeInvalid() throws Exception {
    final UUID user2 = UUID.randomUUID();
    final UUID user3 = UUID.randomUUID();
    final ExternalServiceCredentials user1Cred = credentials(USER_UUID, day(1));
    final ExternalServiceCredentials user2Cred = credentials(user2, day(1));
    final ExternalServiceCredentials user3Cred = credentials(user3, day(1));

    final String fakeToken = token(new ExternalServiceCredentials(user2Cred.username(), user3Cred.password()));
    assertExpectedCredentialCheckResponse(Map.of(
        token(user1Cred), AuthCheckResult.AUTH_CHECK_RESULT_MATCH,
        token(user2Cred), AuthCheckResult.AUTH_CHECK_RESULT_NO_MATCH,
        fakeToken, AuthCheckResult.AUTH_CHECK_RESULT_INVALID
    ), day(2));
  }

  @Test
  public void testSomeExpired() throws Exception {
    final UUID user2 = UUID.randomUUID();
    final UUID user3 = UUID.randomUUID();
    assertExpectedCredentialCheckResponse(Map.of(
        token(USER_UUID, day(100)), AuthCheckResult.AUTH_CHECK_RESULT_MATCH,
        token(user2, day(100)), AuthCheckResult.AUTH_CHECK_RESULT_NO_MATCH,
        token(user3, day(10)), AuthCheckResult.AUTH_CHECK_RESULT_INVALID,
        token(user3, day(20)), AuthCheckResult.AUTH_CHECK_RESULT_INVALID
    ), day(110));
  }

  @Test
  public void testSomeHaveNewerVersions() throws Exception {
    final UUID user2 = UUID.randomUUID();
    final UUID user3 = UUID.randomUUID();
    assertExpectedCredentialCheckResponse(Map.of(
        token(USER_UUID, day(10)), AuthCheckResult.AUTH_CHECK_RESULT_INVALID,
        token(USER_UUID, day(20)), AuthCheckResult.AUTH_CHECK_RESULT_MATCH,
        token(user2, day(10)), AuthCheckResult.AUTH_CHECK_RESULT_NO_MATCH,
        token(user3, day(20)), AuthCheckResult.AUTH_CHECK_RESULT_NO_MATCH,
        token(user3, day(10)), AuthCheckResult.AUTH_CHECK_RESULT_INVALID
    ), day(25));
  }

  private void assertExpectedCredentialCheckResponse(
      final Map<String, AuthCheckResult> expected,
      final long nowMillis) throws Exception {
    CLOCK.setTimeMillis(nowMillis);
    final CheckSvrCredentialsRequest request = CheckSvrCredentialsRequest.newBuilder()
        .setNumber(USER_E164)
        .addAllPasswords(expected.keySet())
        .build();
    final CheckSvrCredentialsResponse response = unauthenticatedServiceStub().checkSvrCredentials(request);
    final Map<String, AuthCheckResult> matchesMap = response.getMatchesMap();
    assertEquals(expected, matchesMap);
  }

  private static String token(final UUID uuid, final long timeMillis) {
    return token(credentials(uuid, timeMillis));
  }

  private static String token(final ExternalServiceCredentials credentials) {
    return credentials.username() + ":" + credentials.password();
  }

  private static ExternalServiceCredentials credentials(final UUID uuid, final long timeMillis) {
    CLOCK.setTimeMillis(timeMillis);
    return SVR_CREDENTIALS_GENERATOR.generateForUuid(uuid);
  }

  private static long day(final int n) {
    return Duration.ofDays(n).toMillis();
  }

  private static Account account(final UUID uuid) {
    final Account a = new Account();
    a.setUuid(uuid);
    return a;
  }
}
