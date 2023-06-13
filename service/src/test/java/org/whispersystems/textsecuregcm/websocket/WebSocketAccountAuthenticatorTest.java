/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import io.dropwizard.auth.basic.BasicCredentials;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.eclipse.jetty.websocket.api.UpgradeRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.auth.AccountAuthenticator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.websocket.auth.WebSocketAuthenticator;

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
        .thenReturn(Optional.of(new AuthenticatedAccount(() -> new Pair<>(mock(Account.class), mock(Device.class)))));

    when(accountAuthenticator.authenticate(eq(new BasicCredentials(INVALID_USER, INVALID_PASSWORD))))
        .thenReturn(Optional.empty());

    upgradeRequest = mock(UpgradeRequest.class);
  }

  @ParameterizedTest
  @MethodSource
  void testAuthenticate(final Map<String, List<String>> upgradeRequestParameters, final boolean expectAccount,
      final boolean expectRequired) throws Exception {

    when(upgradeRequest.getParameterMap()).thenReturn(upgradeRequestParameters);

    final WebSocketAccountAuthenticator webSocketAuthenticator = new WebSocketAccountAuthenticator(
        accountAuthenticator);

    final WebSocketAuthenticator.AuthenticationResult<AuthenticatedAccount> result = webSocketAuthenticator.authenticate(
        upgradeRequest);

    if (expectAccount) {
      assertTrue(result.getUser().isPresent());
    } else {
      assertTrue(result.getUser().isEmpty());
    }

    assertEquals(expectRequired, result.isRequired());
  }

  private static Stream<Arguments> testAuthenticate() {
    return Stream.of(
        Arguments.of(Map.of("login", List.of(VALID_USER), "password", List.of(VALID_PASSWORD)), true, true),
        Arguments.of(Map.of("login", List.of(INVALID_USER), "password", List.of(INVALID_PASSWORD)), false, true),
        Arguments.of(Map.of(), false, false)
    );
  }
}
