/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.commons.lang3.RandomUtils;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureBackupServiceConfiguration;
import org.whispersystems.textsecuregcm.entities.AuthCheckRequest;
import org.whispersystems.textsecuregcm.entities.AuthCheckResponse;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.MutableClock;
import org.whispersystems.textsecuregcm.util.SystemMapper;

@ExtendWith(DropwizardExtensionsSupport.class)
class SecureBackupControllerTest {

  private static final UUID USER_1 = UUID.randomUUID();

  private static final UUID USER_2 = UUID.randomUUID();

  private static final UUID USER_3 = UUID.randomUUID();

  private static final String E164_VALID = "+18005550123";

  private static final String E164_INVALID = "1(800)555-0123";

  private static final byte[] SECRET = RandomUtils.nextBytes(32);

  private static final SecureBackupServiceConfiguration CFG = MockUtils.buildMock(
      SecureBackupServiceConfiguration.class,
      cfg -> Mockito.when(cfg.getUserAuthenticationTokenSharedSecret()).thenReturn(SECRET)
  );
  
  private static final MutableClock CLOCK = MockUtils.mutableClock(0);

  private static final ExternalServiceCredentialsGenerator CREDENTIAL_GENERATOR =
      SecureBackupController.credentialsGenerator(CFG, CLOCK);

  private static final AccountsManager ACCOUNTS_MANAGER = Mockito.mock(AccountsManager.class);

  private static final SecureBackupController CONTROLLER =
      new SecureBackupController(CREDENTIAL_GENERATOR, ACCOUNTS_MANAGER);

  private static final ResourceExtension RESOURCES = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .setMapper(SystemMapper.getMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(CONTROLLER)
      .build();

  @BeforeAll
  public static void before() throws Exception {
    Mockito.when(ACCOUNTS_MANAGER.getByE164(E164_VALID)).thenReturn(Optional.of(account(USER_1)));
  }

  @Test
  public void testOneMatch() throws Exception {
    validate(Map.of(
        token(USER_1, day(1)), AuthCheckResponse.Result.MATCH,
        token(USER_2, day(1)), AuthCheckResponse.Result.NO_MATCH,
        token(USER_3, day(1)), AuthCheckResponse.Result.NO_MATCH
    ), day(2));
  }

  @Test
  public void testNoMatch() throws Exception {
    validate(Map.of(
        token(USER_2, day(1)), AuthCheckResponse.Result.NO_MATCH,
        token(USER_3, day(1)), AuthCheckResponse.Result.NO_MATCH
    ), day(2));
  }

  @Test
  public void testEmptyInput() throws Exception {
    validate(Collections.emptyMap(), day(2));
  }

  @Test
  public void testSomeInvalid() throws Exception {
    final String fakeToken = token(USER_3, day(1)).replaceAll(USER_3.toString(), USER_2.toString());
    validate(Map.of(
        token(USER_1, day(1)), AuthCheckResponse.Result.MATCH,
        token(USER_2, day(1)), AuthCheckResponse.Result.NO_MATCH,
        fakeToken, AuthCheckResponse.Result.INVALID
    ), day(2));
  }

  @Test
  public void testSomeExpired() throws Exception {
    validate(Map.of(
        token(USER_1, day(100)), AuthCheckResponse.Result.MATCH,
        token(USER_2, day(100)), AuthCheckResponse.Result.NO_MATCH,
        token(USER_3, day(10)), AuthCheckResponse.Result.INVALID,
        token(USER_3, day(20)), AuthCheckResponse.Result.INVALID
    ), day(110));
  }

  @Test
  public void testSomeHaveNewerVersions() throws Exception {
    validate(Map.of(
        token(USER_1, day(10)), AuthCheckResponse.Result.INVALID,
        token(USER_1, day(20)), AuthCheckResponse.Result.MATCH,
        token(USER_2, day(10)), AuthCheckResponse.Result.NO_MATCH,
        token(USER_3, day(20)), AuthCheckResponse.Result.NO_MATCH,
        token(USER_3, day(10)), AuthCheckResponse.Result.INVALID
    ), day(25));
  }

  private static void validate(
      final Map<String, AuthCheckResponse.Result> expected,
      final long nowMillis) throws Exception {
    CLOCK.setTimeMillis(nowMillis);
    final AuthCheckRequest request = new AuthCheckRequest(E164_VALID, List.copyOf(expected.keySet()));
    final AuthCheckResponse response = CONTROLLER.authCheck(request);
    assertEquals(expected, response.matches());
  }

  @Test
  public void testHttpResponseCodeSuccess() throws Exception {
    final Map<String, AuthCheckResponse.Result> expected = Map.of(
        token(USER_1, day(10)), AuthCheckResponse.Result.INVALID,
        token(USER_1, day(20)), AuthCheckResponse.Result.MATCH,
        token(USER_2, day(10)), AuthCheckResponse.Result.NO_MATCH,
        token(USER_3, day(20)), AuthCheckResponse.Result.NO_MATCH,
        token(USER_3, day(10)), AuthCheckResponse.Result.INVALID
    );

    CLOCK.setTimeMillis(day(25));

    final AuthCheckRequest in = new AuthCheckRequest(E164_VALID, List.copyOf(expected.keySet()));

    final Response response = RESOURCES.getJerseyTest()
        .target("/v1/backup/auth/check")
        .request()
        .post(Entity.entity(in, MediaType.APPLICATION_JSON));

    try (response) {
      final AuthCheckResponse res = response.readEntity(AuthCheckResponse.class);
      assertEquals(200, response.getStatus());
      assertEquals(expected, res.matches());
    }
  }

  @Test
  public void testHttpResponseCodeWhenInvalidNumber() throws Exception {
    final AuthCheckRequest in = new AuthCheckRequest(E164_INVALID, Collections.singletonList("1"));
    final Response response = RESOURCES.getJerseyTest()
        .target("/v1/backup/auth/check")
        .request()
        .post(Entity.entity(in, MediaType.APPLICATION_JSON));

    try (response) {
      assertEquals(422, response.getStatus());
    }
  }

  @Test
  public void testHttpResponseCodeWhenTooManyTokens() throws Exception {
    final AuthCheckRequest inOkay = new AuthCheckRequest(E164_VALID, List.of(
        "1", "2", "3", "4", "5", "6", "7", "8", "9", "10"
    ));
    final AuthCheckRequest inTooMany = new AuthCheckRequest(E164_VALID, List.of(
        "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11"
    ));
    final AuthCheckRequest inNoTokens = new AuthCheckRequest(E164_VALID, Collections.emptyList());

    final Response responseOkay = RESOURCES.getJerseyTest()
        .target("/v1/backup/auth/check")
        .request()
        .post(Entity.entity(inOkay, MediaType.APPLICATION_JSON));

    final Response responseError1 = RESOURCES.getJerseyTest()
        .target("/v1/backup/auth/check")
        .request()
        .post(Entity.entity(inTooMany, MediaType.APPLICATION_JSON));

    final Response responseError2 = RESOURCES.getJerseyTest()
        .target("/v1/backup/auth/check")
        .request()
        .post(Entity.entity(inNoTokens, MediaType.APPLICATION_JSON));

    try (responseOkay; responseError1; responseError2) {
      assertEquals(200, responseOkay.getStatus());
      assertEquals(422, responseError1.getStatus());
      assertEquals(422, responseError2.getStatus());
    }
  }

  @Test
  public void testHttpResponseCodeWhenPasswordsMissing() throws Exception {
    final Response response = RESOURCES.getJerseyTest()
        .target("/v1/backup/auth/check")
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
  public void testHttpResponseCodeWhenNumberMissing() throws Exception {
    final Response response = RESOURCES.getJerseyTest()
        .target("/v1/backup/auth/check")
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
  public void testHttpResponseCodeWhenExtraFields() throws Exception {
    final Response response = RESOURCES.getJerseyTest()
        .target("/v1/backup/auth/check")
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
  public void testHttpResponseCodeWhenNotAJson() throws Exception {
    final Response response = RESOURCES.getJerseyTest()
        .target("/v1/backup/auth/check")
        .request()
        .post(Entity.entity("random text", MediaType.APPLICATION_JSON));

    try (response) {
      assertEquals(400, response.getStatus());
    }
  }

  private static String token(final UUID uuid, final long timeMillis) {
    CLOCK.setTimeMillis(timeMillis);
    final ExternalServiceCredentials credentials = CREDENTIAL_GENERATOR.generateForUuid(uuid);
    return credentials.username() + ":" + credentials.password();
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
