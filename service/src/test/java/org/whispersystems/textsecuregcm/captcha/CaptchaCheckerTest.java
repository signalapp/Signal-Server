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

import jakarta.ws.rs.BadRequestException;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

public class CaptchaCheckerTest {

  private static final String CHALLENGE_SITE_KEY = "challenge-site-key";
  private static final String REG_SITE_KEY = "registration-site-key";
  private static final String TOKEN = "some-token";
  private static final String PREFIX = "prefix";
  private static final String PREFIX_A = "prefix-a";
  private static final String PREFIX_B = "prefix-b";
  private static final String USER_AGENT = "user-agent";
  private static final UUID ACI = UUID.randomUUID();

  static Stream<Arguments> parseInputToken() {
    return Stream.of(
        Arguments.of(
            String.join(SEPARATOR, PREFIX, CHALLENGE_SITE_KEY, "challenge", TOKEN),
            TOKEN,
            CHALLENGE_SITE_KEY,
            Action.CHALLENGE),
        Arguments.of(
            String.join(SEPARATOR, PREFIX, REG_SITE_KEY, "registration", TOKEN),
            TOKEN,
            REG_SITE_KEY,
            Action.REGISTRATION),
        Arguments.of(
            String.join(SEPARATOR, PREFIX, CHALLENGE_SITE_KEY, "challenge", TOKEN, "something-else"),
            TOKEN + SEPARATOR + "something-else",
            CHALLENGE_SITE_KEY,
            Action.CHALLENGE),
        Arguments.of(
            String.join(SEPARATOR, PREFIX, CHALLENGE_SITE_KEY, "ChAlLeNgE", TOKEN),
            TOKEN,
            CHALLENGE_SITE_KEY,
            Action.CHALLENGE)
    );
  }

  private static CaptchaClient mockClient(final String prefix) throws IOException {
    final CaptchaClient captchaClient = mock(CaptchaClient.class);
    when(captchaClient.scheme()).thenReturn(prefix);
    when(captchaClient.validSiteKeys(eq(Action.CHALLENGE))).thenReturn(Collections.singleton(CHALLENGE_SITE_KEY));
    when(captchaClient.validSiteKeys(eq(Action.REGISTRATION))).thenReturn(Collections.singleton(REG_SITE_KEY));
    when(captchaClient.verify(any(), any(), any(), any(), any(), any())).thenReturn(AssessmentResult.invalid());
    return captchaClient;
  }


  @ParameterizedTest
  @MethodSource
  void parseInputToken(
      final String input,
      final String expectedToken,
      final String siteKey,
      final Action expectedAction) throws IOException {
    final CaptchaClient captchaClient = mockClient(PREFIX);
    new CaptchaChecker(null, PREFIX -> captchaClient).verify(Optional.empty(), expectedAction, input, null, USER_AGENT);
    verify(captchaClient, times(1)).verify(any(), eq(siteKey), eq(expectedAction), eq(expectedToken), any(), eq(USER_AGENT));
  }

  @ParameterizedTest
  @MethodSource
  void scoreString(float score, String expected) {
    assertThat(AssessmentResult.fromScore(score, 0.0f).getScoreString()).isEqualTo(expected);
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
    String ainput = String.join(SEPARATOR, PREFIX_A, CHALLENGE_SITE_KEY, "challenge", TOKEN);
    String binput = String.join(SEPARATOR, PREFIX_B, CHALLENGE_SITE_KEY, "challenge", TOKEN);
    final CaptchaClient a = mockClient(PREFIX_A);
    final CaptchaClient b = mockClient(PREFIX_B);
    final Map<String, CaptchaClient> captchaClientMap = Map.of(PREFIX_A, a, PREFIX_B, b);

    new CaptchaChecker(null, captchaClientMap::get).verify(Optional.of(ACI), Action.CHALLENGE, ainput, null, USER_AGENT);
    verify(a, times(1)).verify(any(), any(), any(), any(), any(), any());

    new CaptchaChecker(null, captchaClientMap::get).verify(Optional.of(ACI), Action.CHALLENGE, binput, null, USER_AGENT);
    verify(b, times(1)).verify(any(), any(), any(), any(), any(), any());
  }

  static Stream<Arguments> badArgs() {
    return Stream.of(
        Arguments.of(String.join(SEPARATOR, "invalid", CHALLENGE_SITE_KEY, "challenge", TOKEN)), // bad prefix
        Arguments.of(String.join(SEPARATOR, PREFIX, "challenge", TOKEN)), // no site key
        Arguments.of(String.join(SEPARATOR, CHALLENGE_SITE_KEY, PREFIX, "challenge", TOKEN)), // incorrect order
        Arguments.of(String.join(SEPARATOR, PREFIX, CHALLENGE_SITE_KEY, "unknown_action", TOKEN)), // bad action
        Arguments.of(String.join(SEPARATOR, PREFIX, CHALLENGE_SITE_KEY, "registration", TOKEN)), // action mismatch
        Arguments.of(String.join(SEPARATOR, PREFIX, "bad-site-key", "challenge", TOKEN)), // invalid site key
        Arguments.of(String.join(SEPARATOR, PREFIX, CHALLENGE_SITE_KEY, "registration", TOKEN)), // site key for wrong type
        Arguments.of(String.join(SEPARATOR, PREFIX, REG_SITE_KEY, "challenge", TOKEN)) // site key for wrong type
    );
  }

  @ParameterizedTest
  @MethodSource
  public void badArgs(final String input) throws IOException {
    final CaptchaClient cc = mockClient(PREFIX);
    assertThrows(BadRequestException.class,
        () -> new CaptchaChecker(null, prefix -> PREFIX.equals(prefix) ? cc : null).verify(Optional.of(ACI), Action.CHALLENGE, input, null, USER_AGENT));

  }

  @Test
  public void testShortened() throws IOException {
    final CaptchaClient captchaClient = mockClient(PREFIX);
    final ShortCodeExpander retriever = mock(ShortCodeExpander.class);
    when(retriever.retrieve("abc")).thenReturn(Optional.of(TOKEN));
    final String input = String.join(SEPARATOR, PREFIX + "-short", REG_SITE_KEY, "registration", "abc");
    new CaptchaChecker(retriever, ignored -> captchaClient).verify(Optional.of(ACI), Action.REGISTRATION, input, null, USER_AGENT);
    verify(captchaClient, times(1)).verify(any(), eq(REG_SITE_KEY), eq(Action.REGISTRATION), eq(TOKEN), any(), any());

  }
}
