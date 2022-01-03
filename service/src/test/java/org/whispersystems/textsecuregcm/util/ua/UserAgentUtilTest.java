/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.ua;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.vdurmont.semver4j.Semver;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class UserAgentUtilTest {

  @ParameterizedTest
  @MethodSource
  void testParseUserAgentString(final String userAgentString, final UserAgent expectedUserAgent)
      throws UnrecognizedUserAgentException {
    assertEquals(expectedUserAgent, UserAgentUtil.parseUserAgentString(userAgentString));
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> testParseUserAgentString() {
    return Stream.of(
        Arguments.of("Signal-Android/4.68.3 Android/25",
            new UserAgent(ClientPlatform.ANDROID, new Semver("4.68.3"), "Android/25")),
        Arguments.of("Signal-Android 4.53.7 (Android 8.1)",
            new UserAgent(ClientPlatform.ANDROID, new Semver("4.53.7"), "(Android 8.1)"))
    );
  }

  @ParameterizedTest
  @MethodSource
  void testParseBogusUserAgentString(final String userAgentString) {
    assertThrows(UnrecognizedUserAgentException.class, () -> UserAgentUtil.parseUserAgentString(userAgentString));
  }

  @SuppressWarnings("unused")
  private static Stream<String> testParseBogusUserAgentString() {
    return Stream.of(
        null,
        "This is obviously not a reasonable User-Agent string.",
        "Signal-Android/4.6-8.3.unreasonableversionstring-17"
    );
  }

  @ParameterizedTest
  @MethodSource("argumentsForTestParseStandardUserAgentString")
  void testParseStandardUserAgentString(final String userAgentString, final UserAgent expectedUserAgent) {
    assertEquals(expectedUserAgent, UserAgentUtil.parseStandardUserAgentString(userAgentString));
  }

  private static Stream<Arguments> argumentsForTestParseStandardUserAgentString() {
    return Stream.of(
        Arguments.of("This is obviously not a reasonable User-Agent string.", null),
        Arguments.of("Signal-Android/4.68.3 Android/25",
            new UserAgent(ClientPlatform.ANDROID, new Semver("4.68.3"), "Android/25")),
        Arguments.of("Signal-Android/4.68.3", new UserAgent(ClientPlatform.ANDROID, new Semver("4.68.3"))),
        Arguments.of("Signal-Desktop/1.2.3 Linux", new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"), "Linux")),
        Arguments.of("Signal-Desktop/1.2.3 macOS", new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"), "macOS")),
        Arguments.of("Signal-Desktop/1.2.3 Windows",
            new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"), "Windows")),
        Arguments.of("Signal-Desktop/1.2.3", new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"))),
        Arguments.of("Signal-Desktop/1.32.0-beta.3",
            new UserAgent(ClientPlatform.DESKTOP, new Semver("1.32.0-beta.3"))),
        Arguments.of("Signal-iOS/3.9.0 (iPhone; iOS 12.2; Scale/3.00)",
            new UserAgent(ClientPlatform.IOS, new Semver("3.9.0"), "(iPhone; iOS 12.2; Scale/3.00)")),
        Arguments.of("Signal-iOS/3.9.0 iOS/14.2", new UserAgent(ClientPlatform.IOS, new Semver("3.9.0"), "iOS/14.2")),
        Arguments.of("Signal-iOS/3.9.0", new UserAgent(ClientPlatform.IOS, new Semver("3.9.0")))
    );
  }

  @ParameterizedTest
  @MethodSource
  void testParseLegacyUserAgentString(final String userAgentString, final UserAgent expectedUserAgent) {
    assertEquals(expectedUserAgent, UserAgentUtil.parseLegacyUserAgentString(userAgentString));
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> testParseLegacyUserAgentString() {
    return Stream.of(
        Arguments.of("This is obviously not a reasonable User-Agent string.", null),
        Arguments.of("Signal-Android 4.53.7 (Android 8.1)",
            new UserAgent(ClientPlatform.ANDROID, new Semver("4.53.7"), "(Android 8.1)")),
        Arguments.of("Signal Desktop 1.2.3", new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"))),
        Arguments.of("Signal Desktop 1.32.0-beta.3",
            new UserAgent(ClientPlatform.DESKTOP, new Semver("1.32.0-beta.3"))),
        Arguments.of("Signal/3.9.0 (iPhone; iOS 12.2; Scale/3.00)",
            new UserAgent(ClientPlatform.IOS, new Semver("3.9.0"), "(iPhone; iOS 12.2; Scale/3.00)"))
    );
  }
}
