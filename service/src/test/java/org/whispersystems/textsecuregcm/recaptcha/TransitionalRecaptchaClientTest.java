/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.recaptcha;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.whispersystems.textsecuregcm.recaptcha.EnterpriseRecaptchaClient.SEPARATOR;

import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class TransitionalRecaptchaClientTest {

  private TransitionalRecaptchaClient transitionalRecaptchaClient;
  private EnterpriseRecaptchaClient enterpriseRecaptchaClient;
  private LegacyRecaptchaClient legacyRecaptchaClient;

  private static final String PREFIX = TransitionalRecaptchaClient.V2_PREFIX.substring(0,
      TransitionalRecaptchaClient.V2_PREFIX.lastIndexOf(SEPARATOR));
  private static final String TOKEN = "some-token";
  private static final String IP_ADDRESS = "127.0.0.1";

  @BeforeEach
  void setup() {
    enterpriseRecaptchaClient = mock(EnterpriseRecaptchaClient.class);
    legacyRecaptchaClient = mock(LegacyRecaptchaClient.class);
    transitionalRecaptchaClient = new TransitionalRecaptchaClient(legacyRecaptchaClient, enterpriseRecaptchaClient);
  }

  @ParameterizedTest
  @MethodSource
  void testVerify(final String inputToken, final boolean expectLegacy, final String expectedToken) {

    transitionalRecaptchaClient.verify(inputToken, IP_ADDRESS);

    if (expectLegacy) {
      verifyNoInteractions(enterpriseRecaptchaClient);
      verify(legacyRecaptchaClient).verify(expectedToken, IP_ADDRESS);
    } else {
      verifyNoInteractions(legacyRecaptchaClient);
      verify(enterpriseRecaptchaClient).verify(expectedToken, IP_ADDRESS);
    }

  }

  static Stream<Arguments> testVerify() {
    return Stream.of(
        Arguments.of(
            TOKEN,
            true,
            TOKEN),
        Arguments.of(
            String.join(SEPARATOR, PREFIX, TOKEN),
            false,
            TOKEN),
        Arguments.of(
            String.join(SEPARATOR, PREFIX, "site-key", "an-action", TOKEN),
            false,
            String.join(SEPARATOR, "site-key", "an-action", TOKEN),
            "an-action"),
        Arguments.of(
            String.join(SEPARATOR, PREFIX, "site-key", "an-action", TOKEN, "something-else"),
            false,
            "site-key" + SEPARATOR + "an-action" + SEPARATOR + TOKEN + SEPARATOR + "something-else")
    );
  }

}
