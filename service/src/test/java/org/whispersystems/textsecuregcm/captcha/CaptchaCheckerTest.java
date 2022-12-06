/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.captcha;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.captcha.CaptchaChecker.SEPARATOR;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import javax.ws.rs.BadRequestException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CaptchaCheckerTest {

  private static final String SITE_KEY = "site-key";
  private static final String TOKEN = "some-token";
  private static final String PREFIX = "prefix";
  private static final String PREFIX_A = "prefix-a";
  private static final String PREFIX_B = "prefix-b";

  static Stream<Arguments> parseInputToken() {
    return Stream.of(
        Arguments.of(
            String.join(SEPARATOR, PREFIX, SITE_KEY, TOKEN),
            TOKEN,
            SITE_KEY,
            null),
        Arguments.of(
            String.join(SEPARATOR, PREFIX, SITE_KEY, "an-action", TOKEN),
            TOKEN,
            SITE_KEY,
            "an-action"),
        Arguments.of(
            String.join(SEPARATOR, PREFIX, SITE_KEY, "an-action", TOKEN, "something-else"),
            TOKEN + SEPARATOR + "something-else",
            SITE_KEY,
            "an-action")
    );
  }

  private static CaptchaClient mockClient(final String prefix) throws IOException {
    final CaptchaClient captchaClient = mock(CaptchaClient.class);
    when(captchaClient.scheme()).thenReturn(prefix);
    when(captchaClient.verify(any(), any(), any(), any())).thenReturn(AssessmentResult.invalid());
    return captchaClient;
  }


  @ParameterizedTest
  @MethodSource
  void parseInputToken(final String input, final String expectedToken, final String siteKey,
      @Nullable final String expectedAction) throws IOException {
    final CaptchaClient captchaClient = mockClient(PREFIX);
    new CaptchaChecker(List.of(captchaClient)).verify(input, null);
    verify(captchaClient, times(1)).verify(eq(siteKey), eq(expectedAction), eq(expectedToken), any());
  }

  @ParameterizedTest
  @MethodSource
  void scoreString(float score, String expected) {
    assertThat(AssessmentResult.scoreString(score)).isEqualTo(expected);
  }


  static Stream<Arguments> scoreString() {
    return Stream.of(
        Arguments.of(0.3f, "30"),
        Arguments.of(0.0f, "0"),
        Arguments.of(0.333f, "30"),
        Arguments.of(0.29f, "30"),
        Arguments.of(Float.NaN, "0")
    );
  }

  @Test
  public void choose() throws IOException {
    String ainput = String.join(SEPARATOR, PREFIX_A, SITE_KEY, TOKEN);
    String binput = String.join(SEPARATOR, PREFIX_B, SITE_KEY, TOKEN);
    final CaptchaClient a = mockClient(PREFIX_A);
    final CaptchaClient b = mockClient(PREFIX_B);

    new CaptchaChecker(List.of(a, b)).verify(ainput, null);
    verify(a, times(1)).verify(any(), any(), any(), any());

    new CaptchaChecker(List.of(a, b)).verify(binput, null);
    verify(b, times(1)).verify(any(), any(), any(), any());
  }

  static Stream<Arguments> badToken() {
    return Stream.of(
        Arguments.of(String.join(SEPARATOR, "invalid", SITE_KEY, "action", TOKEN)),
        Arguments.of(String.join(SEPARATOR, PREFIX, TOKEN)),
        Arguments.of(String.join(SEPARATOR, SITE_KEY, PREFIX, "action", TOKEN))
    );
  }

  @ParameterizedTest
  @MethodSource
  public void badToken(final String input) throws IOException {
    final CaptchaClient cc = mockClient(PREFIX);
    assertThrows(BadRequestException.class, () -> new CaptchaChecker(List.of(cc)).verify(input, null));

  }
}
