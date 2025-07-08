/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.whispersystems.textsecuregcm.util.MockUtils.randomSecretBytes;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureValueRecoveryConfiguration;
import org.whispersystems.textsecuregcm.entities.AuthCheckRequest;
import org.whispersystems.textsecuregcm.entities.AuthCheckResponseV2;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.MutableClock;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@ExtendWith(DropwizardExtensionsSupport.class)
public class SecureValueRecovery2ControllerTest {

  private static final SecureValueRecoveryConfiguration CFG = new SecureValueRecoveryConfiguration(
      "",
      randomSecretBytes(32),
      randomSecretBytes(32),
      null,
      null,
      null
  );

  private static final MutableClock CLOCK = new MutableClock();

  private static final ExternalServiceCredentialsGenerator CREDENTIAL_GENERATOR =
      SecureValueRecovery2Controller.credentialsGenerator(CFG, CLOCK);

  private static final AccountsManager ACCOUNTS_MANAGER = mock(AccountsManager.class);
  private static final SecureValueRecovery2Controller CONTROLLER =
      new SecureValueRecovery2Controller(CREDENTIAL_GENERATOR, ACCOUNTS_MANAGER);

  private static final ResourceExtension RESOURCES = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(CONTROLLER)
      .build();

  @Nested
  class WithBackupsPrefix extends SecureValueRecoveryControllerBaseTest {
    protected WithBackupsPrefix() {
      super("/v2/backup");
    }
  }

  @Nested
  class WithSvr2Prefix extends SecureValueRecoveryControllerBaseTest {
    protected WithSvr2Prefix() {
      super("/v2/svr");
    }
  }

  static abstract class SecureValueRecoveryControllerBaseTest {
    private static final UUID USER_1 = UUID.randomUUID();
    private static final UUID USER_2 = UUID.randomUUID();
    private static final UUID USER_3 = UUID.randomUUID();
    private static final String E164_VALID = "+18005550123";
    private static final String E164_INVALID = "1(800)555-0123";

    private final String pathPrefix;

    @BeforeEach
    public void before() throws Exception {
      Mockito.reset(ACCOUNTS_MANAGER);
      Mockito.when(ACCOUNTS_MANAGER.getByE164(E164_VALID)).thenReturn(Optional.of(account(USER_1)));
    }

    protected SecureValueRecoveryControllerBaseTest(final String pathPrefix) {
      this.pathPrefix = pathPrefix;
    }

    enum CheckStatus {
      MATCH,
      NO_MATCH,
      INVALID
    }

    private Map<String, CheckStatus> parseCheckResponse(final Response response) {
      final AuthCheckResponseV2 authCheckResponseV2 = response.readEntity(AuthCheckResponseV2.class);
      return authCheckResponseV2.matches().entrySet().stream().collect(Collectors.toMap(
          Map.Entry::getKey, e -> switch (e.getValue()) {
            case MATCH -> CheckStatus.MATCH;
            case INVALID -> CheckStatus.INVALID;
            case NO_MATCH -> CheckStatus.NO_MATCH;
          }
      ));
    }

    @Test
    public void testOneMatch() {
      validate(Map.of(
          token(USER_1, day(1)), CheckStatus.MATCH,
          token(USER_2, day(1)), CheckStatus.NO_MATCH,
          token(USER_3, day(1)), CheckStatus.NO_MATCH
      ), day(2));
    }

    @Test
    public void testNoMatch() {
      validate(Map.of(
          token(USER_2, day(1)), CheckStatus.NO_MATCH,
          token(USER_3, day(1)), CheckStatus.NO_MATCH
      ), day(2));
    }

    @Test
    public void testSomeInvalid() {
      final ExternalServiceCredentials user1Cred = credentials(USER_1, day(1));
      final ExternalServiceCredentials user2Cred = credentials(USER_2, day(1));
      final ExternalServiceCredentials user3Cred = credentials(USER_3, day(1));

      final String fakeToken = token(new ExternalServiceCredentials(user2Cred.username(), user3Cred.password()));
      validate(Map.of(
          token(user1Cred), CheckStatus.MATCH,
          token(user2Cred), CheckStatus.NO_MATCH,
          fakeToken, CheckStatus.INVALID
      ), day(2));
    }

    @Test
    public void testSomeExpired() {
      validate(Map.of(
          token(USER_1, day(100)), CheckStatus.MATCH,
          token(USER_2, day(100)), CheckStatus.NO_MATCH,
          token(USER_3, day(10)), CheckStatus.INVALID,
          token(USER_3, day(20)), CheckStatus.INVALID
      ), day(110));
    }

    @Test
    public void testSomeHaveNewerVersions() {
      validate(Map.of(
          token(USER_1, day(10)), CheckStatus.INVALID,
          token(USER_1, day(20)), CheckStatus.MATCH,
          token(USER_2, day(10)), CheckStatus.NO_MATCH,
          token(USER_3, day(20)), CheckStatus.NO_MATCH,
          token(USER_3, day(10)), CheckStatus.INVALID
      ), day(25));
    }

    private void validate(
        final Map<String, CheckStatus> expected,
        final long nowMillis) {
      CLOCK.setTimeMillis(nowMillis);
      final AuthCheckRequest request = new AuthCheckRequest(E164_VALID, List.copyOf(expected.keySet()));
      final Response response = RESOURCES.getJerseyTest().target(pathPrefix + "/auth/check")
          .request()
          .post(Entity.entity(request, MediaType.APPLICATION_JSON));
      try (response) {
        assertEquals(200, response.getStatus());
        final Map<String, CheckStatus> res = parseCheckResponse(response);
        assertEquals(expected, res);
      }
    }

    @Test
    public void testHttpResponseCodeSuccess() {
      final Map<String, CheckStatus> expected = Map.of(
          token(USER_1, day(10)), CheckStatus.INVALID,
          token(USER_1, day(20)), CheckStatus.MATCH,
          token(USER_2, day(10)), CheckStatus.NO_MATCH,
          token(USER_3, day(20)), CheckStatus.NO_MATCH,
          token(USER_3, day(10)), CheckStatus.INVALID
      );

      CLOCK.setTimeMillis(day(25));

      final AuthCheckRequest in = new AuthCheckRequest(E164_VALID, List.copyOf(expected.keySet()));

      final Response response = RESOURCES.getJerseyTest()
          .target(pathPrefix + "/auth/check")
          .request()
          .post(Entity.entity(in, MediaType.APPLICATION_JSON));

      try (response) {
        assertEquals(200, response.getStatus());
        assertEquals(expected, parseCheckResponse(response));
      }
    }

    @Test
    public void testHttpResponseCodeWhenInvalidNumber() {
      final AuthCheckRequest in = new AuthCheckRequest(E164_INVALID, Collections.singletonList("1"));
      final Response response = RESOURCES.getJerseyTest()
          .target(pathPrefix + "/auth/check")
          .request()
          .post(Entity.entity(in, MediaType.APPLICATION_JSON));

      try (response) {
        assertEquals(422, response.getStatus());
      }
    }

    @Test
    public void testHttpResponseCodeWhenTooManyTokens() {
      final AuthCheckRequest inOkay = new AuthCheckRequest(E164_VALID, List.of(
          "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"
      ));
      final AuthCheckRequest inTooMany = new AuthCheckRequest(E164_VALID, List.of(
          "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"
      ));
      final AuthCheckRequest inNoTokens = new AuthCheckRequest(E164_VALID, Collections.emptyList());

      final Response responseOkay = RESOURCES.getJerseyTest()
          .target(pathPrefix + "/auth/check")
          .request()
          .post(Entity.entity(inOkay, MediaType.APPLICATION_JSON));

      final Response responseError1 = RESOURCES.getJerseyTest()
          .target(pathPrefix + "/auth/check")
          .request()
          .post(Entity.entity(inTooMany, MediaType.APPLICATION_JSON));

      final Response responseError2 = RESOURCES.getJerseyTest()
          .target(pathPrefix + "/auth/check")
          .request()
          .post(Entity.entity(inNoTokens, MediaType.APPLICATION_JSON));

      try (responseOkay; responseError1; responseError2) {
        assertEquals(200, responseOkay.getStatus());
        assertEquals(422, responseError1.getStatus());
        assertEquals(422, responseError2.getStatus());
      }
    }

    @Test
    public void testHttpResponseCodeWhenPasswordsMissing() {
      final Response response = RESOURCES.getJerseyTest()
          .target(pathPrefix + "/auth/check")
          .request()
          .post(Entity.entity("""
            {
              "number": "123"
            }
            """, MediaType.APPLICATION_JSON));

      try (response) {
        assertEquals(422, response.getStatus());
      }
    }

    @Test
    public void testHttpResponseCodeWhenNumberMissing() {
      final Response response = RESOURCES.getJerseyTest()
          .target(pathPrefix + "/auth/check")
          .request()
          .post(Entity.entity("""
            {
              "passwords": ["aaa:bbb"]
            }
            """, MediaType.APPLICATION_JSON));

      try (response) {
        assertEquals(422, response.getStatus());
      }
    }

    @Test
    public void testHttpResponseCodeWhenExtraFields() {
      final Response response = RESOURCES.getJerseyTest()
          .target(pathPrefix + "/auth/check")
          .request()
          .post(Entity.entity("""
            {
              "number": "+18005550123",
              "passwords": ["aaa:bbb"],
              "unexpected": "value"
            }
            """, MediaType.APPLICATION_JSON));

      try (response) {
        assertEquals(200, response.getStatus());
      }
    }

    @Test
    public void testAcceptsPasswordsOrTokens() {
      final Response passwordsResponse = RESOURCES.getJerseyTest()
          .target(pathPrefix + "/auth/check")
          .request()
          .post(Entity.entity("""
            {
              "number": "+18005550123",
              "passwords": ["aaa:bbb"]
            }
            """, MediaType.APPLICATION_JSON));
      try (passwordsResponse) {
        assertEquals(200, passwordsResponse.getStatus());
      }

      final Response tokensResponse = RESOURCES.getJerseyTest()
          .target(pathPrefix + "/auth/check")
          .request()
          .post(Entity.entity("""
            {
              "number": "+18005550123",
              "tokens": ["aaa:bbb"]
            }
            """, MediaType.APPLICATION_JSON));
      try (tokensResponse) {
        assertEquals(200, tokensResponse.getStatus());
      }
    }

    @Test
    public void testHttpResponseCodeWhenNotAJson() {
      final Response response = RESOURCES.getJerseyTest()
          .target(pathPrefix + "/auth/check")
          .request()
          .post(Entity.entity("random text", MediaType.APPLICATION_JSON));

      try (response) {
        assertEquals(400, response.getStatus());
      }
    }

    private String token(final UUID uuid, final long timeMillis) {
      return token(credentials(uuid, timeMillis));
    }

    private static String token(final ExternalServiceCredentials credentials) {
      return credentials.username() + ":" + credentials.password();
    }

    private ExternalServiceCredentials credentials(final UUID uuid, final long timeMillis) {
      CLOCK.setTimeMillis(timeMillis);
      return CREDENTIAL_GENERATOR.generateForUuid(uuid);
    }

    private static long day(final int n) {
      return TimeUnit.DAYS.toMillis(n);
    }

    private static Account account(final UUID uuid) {
      final Account a = new Account();
      a.setUuid(uuid);
      return a;
    }
  }
}
