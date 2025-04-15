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
import javax.annotation.Nullable;

class UserAgentUtilTest {

  @ParameterizedTest
  @MethodSource("argumentsForTestParseStandardUserAgentString")
  void testParseStandardUserAgentString(final String userAgentString, @Nullable final UserAgent expectedUserAgent)
      throws UnrecognizedUserAgentException {

    if (expectedUserAgent != null) {
      assertEquals(expectedUserAgent, UserAgentUtil.parseUserAgentString(userAgentString));
    } else {
      assertThrows(UnrecognizedUserAgentException.class, () -> UserAgentUtil.parseUserAgentString(userAgentString));
    }
  }

  private static Stream<Arguments> argumentsForTestParseStandardUserAgentString() {
    return Stream.of(
        Arguments.of("This is obviously not a reasonable User-Agent string.", null),
        Arguments.of("Signal-Android/4.68.3 Android/25",
            new UserAgent(ClientPlatform.ANDROID, new Semver("4.68.3"), "Android/25")),
        Arguments.of("Signal-Android/4.68.3", new UserAgent(ClientPlatform.ANDROID, new Semver("4.68.3"), null)),
        Arguments.of("Signal-Desktop/1.2.3 Linux", new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"), "Linux")),
        Arguments.of("Signal-Desktop/1.2.3 macOS", new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"), "macOS")),
        Arguments.of("Signal-Desktop/1.2.3 Windows",
            new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"), "Windows")),
        Arguments.of("Signal-Desktop/1.2.3", new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"), null)),
        Arguments.of("Signal-Desktop/1.32.0-beta.3",
            new UserAgent(ClientPlatform.DESKTOP, new Semver("1.32.0-beta.3"), null)),
        Arguments.of("Signal-iOS/3.9.0 (iPhone; iOS 12.2; Scale/3.00)",
            new UserAgent(ClientPlatform.IOS, new Semver("3.9.0"), "(iPhone; iOS 12.2; Scale/3.00)")),
        Arguments.of("Signal-iOS/3.9.0 iOS/14.2", new UserAgent(ClientPlatform.IOS, new Semver("3.9.0"), "iOS/14.2")),
        Arguments.of("Signal-iOS/3.9.0", new UserAgent(ClientPlatform.IOS, new Semver("3.9.0"), null)),
        Arguments.of("Signal-Android/7.11.23-nightly-1982-06-28-07-07-07 tonic/0.31",
            new UserAgent(ClientPlatform.ANDROID, new Semver("7.11.23-nightly-1982-06-28-07-07-07"), "tonic/0.31")),
        Arguments.of("Signal-Android/7.11.23-nightly-1982-06-28-07-07-07 Android/42 tonic/0.31",
            new UserAgent(ClientPlatform.ANDROID, new Semver("7.11.23-nightly-1982-06-28-07-07-07"), "Android/42 tonic/0.31")),
        Arguments.of("Signal-Android/7.6.2 Android/34 libsignal/0.46.0",
            new UserAgent(ClientPlatform.ANDROID, new Semver("7.6.2"), "Android/34 libsignal/0.46.0")));
  }
}
