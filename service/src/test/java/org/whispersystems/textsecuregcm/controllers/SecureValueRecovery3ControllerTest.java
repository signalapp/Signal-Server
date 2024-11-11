/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;


import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.util.MockUtils.randomSecretBytes;

import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.glassfish.jersey.test.grizzly.GrizzlyWebTestContainerFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.SecureValueRecovery3Configuration;
import org.whispersystems.textsecuregcm.entities.AuthCheckRequest;
import org.whispersystems.textsecuregcm.entities.AuthCheckResponseV3;
import org.whispersystems.textsecuregcm.entities.SetShareSetRequest;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.util.MutableClock;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

@ExtendWith(DropwizardExtensionsSupport.class)
public class SecureValueRecovery3ControllerTest extends SecureValueRecoveryControllerBaseTest {

  private static final SecureValueRecovery3Configuration CFG = new SecureValueRecovery3Configuration(
      "",
      randomSecretBytes(32),
      randomSecretBytes(32),
      null,
      null,
      null
  );

  private static final MutableClock CLOCK = new MutableClock();

  private static final ExternalServiceCredentialsGenerator CREDENTIAL_GENERATOR =
      SecureValueRecovery3Controller.credentialsGenerator(CFG, CLOCK);

  private static final AccountsManager ACCOUNTS_MANAGER = mock(AccountsManager.class);
  private static final SecureValueRecovery3Controller CONTROLLER =
      new SecureValueRecovery3Controller(CREDENTIAL_GENERATOR, ACCOUNTS_MANAGER);

  private static final ResourceExtension RESOURCES = ResourceExtension.builder()
      .addProvider(AuthHelper.getAuthFilter())
      .addProvider(new AuthValueFactoryProvider.Binder<>(AuthenticatedDevice.class))
      .setMapper(SystemMapper.jsonMapper())
      .setTestContainerFactory(new GrizzlyWebTestContainerFactory())
      .addResource(CONTROLLER)
      .build();

  protected SecureValueRecovery3ControllerTest() {
    super("/v3", ACCOUNTS_MANAGER, CLOCK, RESOURCES, CREDENTIAL_GENERATOR);
  }

  @Override
  Map<String, CheckStatus> parseCheckResponse(final Response response) {
    final AuthCheckResponseV3 authCheckResponse = response.readEntity(AuthCheckResponseV3.class);

    assertFalse(authCheckResponse.matches()
            .values().stream()
            .anyMatch(r -> r.status() == AuthCheckResponseV3.CredentialStatus.MATCH && r.shareSet() == null),
        "SVR3 matches must contain a non-empty share-set");

    return authCheckResponse.matches().entrySet().stream().collect(Collectors.toMap(
        Map.Entry::getKey, e -> switch (e.getValue().status()) {
          case MATCH -> CheckStatus.MATCH;
          case INVALID -> CheckStatus.INVALID;
          case NO_MATCH -> CheckStatus.NO_MATCH;
        }
    ));
  }


  public static Stream<Arguments> checkShareSet() {
    byte[] shareSet = TestRandomUtil.nextBytes(100);
    return Stream.of(
        Arguments.of(shareSet, AuthCheckResponseV3.Result.match(shareSet)),
        Arguments.of(null, AuthCheckResponseV3.Result.match(null)));
  }

  @ParameterizedTest
  @MethodSource
  public void checkShareSet(@Nullable byte[] shareSet, AuthCheckResponseV3.Result expectedResult) {
    final String e164 = "+18005550101";
    final UUID uuid = UUID.randomUUID();
    final String token = token(uuid, day(10));
    CLOCK.setTimeMillis(day(11));

    final Account a = mock(Account.class);
    when(a.getUuid()).thenReturn(uuid);
    when(a.getSvr3ShareSet()).thenReturn(shareSet);
    when(ACCOUNTS_MANAGER.getByE164(e164)).thenReturn(Optional.of(a));

    final AuthCheckRequest in = new AuthCheckRequest(e164, Collections.singletonList(token));
    final Response response = RESOURCES.getJerseyTest()
        .target("/v3/backup/auth/check")
        .request()
        .post(Entity.entity(in, MediaType.APPLICATION_JSON));

    try (response) {
      assertEquals(200, response.getStatus());
      AuthCheckResponseV3 checkResponse = response.readEntity(AuthCheckResponseV3.class);
      assertEquals(checkResponse.matches().size(), 1);
      assertEquals(checkResponse.matches().get(token).status(), expectedResult.status());
      assertArrayEquals(checkResponse.matches().get(token).shareSet(), expectedResult.shareSet());
    }
  }

  @Test
  public void setShareSet() {
    final Account a = mock(Account.class);
    when(ACCOUNTS_MANAGER.update(any(), any())).thenAnswer(invocation -> {
      final Consumer<Account> updater = invocation.getArgument(1);
      updater.accept(a);
      return null;
    });

    byte[] shareSet = TestRandomUtil.nextBytes(SetShareSetRequest.SHARE_SET_SIZE);
    final Response response = RESOURCES.getJerseyTest()
        .target("/v3/backup/share-set")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity(new SetShareSetRequest(shareSet), MediaType.APPLICATION_JSON));

    assertEquals(204, response.getStatus());
    verify(a, times(1)).setSvr3ShareSet(eq(shareSet));
  }

  static Stream<Arguments> requestParsing() {
    return Stream.of(
        Arguments.of("", 422),
        Arguments.of(null, 422),
        Arguments.of("abc**", 400), // bad base64
        Arguments.of(Base64.getEncoder().encodeToString(TestRandomUtil.nextBytes(SetShareSetRequest.SHARE_SET_SIZE - 1)), 422),
        Arguments.of(Base64.getEncoder().encodeToString(TestRandomUtil.nextBytes(SetShareSetRequest.SHARE_SET_SIZE)), 204));
  }

  @ParameterizedTest
  @MethodSource
  public void requestParsing(String shareSet, int responseCode) {
    final Response response = RESOURCES.getJerseyTest()
        .target("/v3/backup/share-set")
        .request()
        .header("Authorization", AuthHelper.getAuthHeader(AuthHelper.VALID_UUID, AuthHelper.VALID_PASSWORD))
        .put(Entity.entity("""
            {"shareSet": "%s"}
            """.formatted(shareSet), MediaType.APPLICATION_JSON));
    assertEquals(responseCode, response.getStatus());
  }
}
