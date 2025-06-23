/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.net.HttpHeaders;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import io.dropwizard.auth.basic.BasicCredentials;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import org.whispersystems.websocket.auth.InvalidCredentialsException;

class WebSocketAccountAuthenticatorTest {

  private static final String VALID_USER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("NZ"), PhoneNumberUtil.PhoneNumberFormat.E164);

  private static final String VALID_PASSWORD = "valid";

  private static final String INVALID_USER = PhoneNumberUtil.getInstance().format(
      PhoneNumberUtil.getInstance().getExampleNumber("AU"), PhoneNumberUtil.PhoneNumberFormat.E164);

  private static final String INVALID_PASSWORD = "invalid";

  private AccountAuthenticator accountAuthenticator;

  private UpgradeRequest upgradeRequest;

  @BeforeEach
  void setUp() {
    accountAuthenticator = mock(AccountAuthenticator.class);

    when(accountAuthenticator.authenticate(eq(new BasicCredentials(VALID_USER, VALID_PASSWORD))))
        .thenReturn(Optional.of(new AuthenticatedDevice(UUID.randomUUID(), Device.PRIMARY_ID, Instant.now())));

    when(accountAuthenticator.authenticate(eq(new BasicCredentials(INVALID_USER, INVALID_PASSWORD))))
        .thenReturn(Optional.empty());

    upgradeRequest = mock(UpgradeRequest.class);
  }

  @ParameterizedTest
  @MethodSource
  void testAuthenticate(
      @Nullable final String authorizationHeaderValue,
      final boolean expectAccount,
      final boolean expectInvalid) throws Exception {

    if (authorizationHeaderValue != null) {
      when(upgradeRequest.getHeader(eq(HttpHeaders.AUTHORIZATION))).thenReturn(authorizationHeaderValue);
    }

    final WebSocketAccountAuthenticator webSocketAuthenticator = new WebSocketAccountAuthenticator(accountAuthenticator);

    if (expectInvalid) {
      assertThrows(InvalidCredentialsException.class, () -> webSocketAuthenticator.authenticate(upgradeRequest));
    } else {
      assertEquals(expectAccount, webSocketAuthenticator.authenticate(upgradeRequest).isPresent());
    }
  }

  private static Stream<Arguments> testAuthenticate() {
    final String headerWithValidAuth =
        HeaderUtils.basicAuthHeader(VALID_USER, VALID_PASSWORD);
    final String headerWithInvalidAuth =
        HeaderUtils.basicAuthHeader(INVALID_USER, INVALID_PASSWORD);
    return Stream.of(
        Arguments.of(headerWithValidAuth, true, false),
        Arguments.of(headerWithInvalidAuth, false, true),
        Arguments.of("invalid header value", false, true),
        // if `Authorization` header is not set, we expect no account and anonymous credentials
        Arguments.of(null, false, false)
    );
  }
}
