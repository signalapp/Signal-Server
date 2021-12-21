/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.ua;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.vdurmont.semver4j.Semver;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class UserAgentUtilTest {

    @ParameterizedTest
    @MethodSource("argumentsForTestParseUserAgentString")
    void testParseUserAgentString(final String userAgentString, final UserAgent expectedUserAgent) throws UnrecognizedUserAgentException {
        assertEquals(expectedUserAgent, UserAgentUtil.parseUserAgentString(userAgentString));
    }

    private static Object[] argumentsForTestParseUserAgentString() {
        return new Object[] {
                new Object[] { "Signal-Android/4.68.3 Android/25",    new UserAgent(ClientPlatform.ANDROID, new Semver("4.68.3"), "Android/25") },
                new Object[] { "Signal-Android 4.53.7 (Android 8.1)", new UserAgent(ClientPlatform.ANDROID, new Semver("4.53.7"), "(Android 8.1)") },
        };
    }

    @ParameterizedTest
    @MethodSource("argumentsForTestParseBogusUserAgentString")
    void testParseBogusUserAgentString(final String userAgentString) {
        assertThrows(UnrecognizedUserAgentException.class, () -> UserAgentUtil.parseUserAgentString(userAgentString));
    }

    private static Object[] argumentsForTestParseBogusUserAgentString() {
        return new Object[] {
                null,
                "This is obviously not a reasonable User-Agent string.",
                "Signal-Android/4.6-8.3.unreasonableversionstring-17"
        };
    }

    @ParameterizedTest
    @MethodSource("argumentsForTestParseStandardUserAgentString")
    void testParseStandardUserAgentString(final String userAgentString, final UserAgent expectedUserAgent) {
        assertEquals(expectedUserAgent, UserAgentUtil.parseStandardUserAgentString(userAgentString));
    }

    private static Object[] argumentsForTestParseStandardUserAgentString() {
        return new Object[] {
                new Object[] { "This is obviously not a reasonable User-Agent string.", null },
                new Object[] { "Signal-Android/4.68.3 Android/25",                      new UserAgent(ClientPlatform.ANDROID, new Semver("4.68.3"), "Android/25") },
                new Object[] { "Signal-Android/4.68.3",                                 new UserAgent(ClientPlatform.ANDROID, new Semver("4.68.3")) },
                new Object[] { "Signal-Desktop/1.2.3 Linux",                            new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"), "Linux") },
                new Object[] { "Signal-Desktop/1.2.3 macOS",                            new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"), "macOS") },
                new Object[] { "Signal-Desktop/1.2.3 Windows",                          new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3"), "Windows") },
                new Object[] { "Signal-Desktop/1.2.3",                                  new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3")) },
                new Object[] { "Signal-Desktop/1.32.0-beta.3",                          new UserAgent(ClientPlatform.DESKTOP, new Semver("1.32.0-beta.3")) },
                new Object[] { "Signal-iOS/3.9.0 (iPhone; iOS 12.2; Scale/3.00)",       new UserAgent(ClientPlatform.IOS, new Semver("3.9.0"), "(iPhone; iOS 12.2; Scale/3.00)") },
                new Object[] { "Signal-iOS/3.9.0 iOS/14.2",                             new UserAgent(ClientPlatform.IOS, new Semver("3.9.0"), "iOS/14.2") },
                new Object[] { "Signal-iOS/3.9.0",                                      new UserAgent(ClientPlatform.IOS, new Semver("3.9.0")) }
        };
    }

    @ParameterizedTest
    @MethodSource("argumentsForTestParseLegacyUserAgentString")
    void testParseLegacyUserAgentString(final String userAgentString, final UserAgent expectedUserAgent) {
        assertEquals(expectedUserAgent, UserAgentUtil.parseLegacyUserAgentString(userAgentString));
    }

    private static Object[] argumentsForTestParseLegacyUserAgentString() {
        return new Object[] {
                new Object[] { "This is obviously not a reasonable User-Agent string.", null },
                new Object[] { "Signal-Android 4.53.7 (Android 8.1)",                   new UserAgent(ClientPlatform.ANDROID, new Semver("4.53.7"), "(Android 8.1)") },
                new Object[] { "Signal Desktop 1.2.3",                                  new UserAgent(ClientPlatform.DESKTOP, new Semver("1.2.3")) },
                new Object[] { "Signal Desktop 1.32.0-beta.3",                          new UserAgent(ClientPlatform.DESKTOP, new Semver("1.32.0-beta.3")) },
                new Object[] { "Signal/3.9.0 (iPhone; iOS 12.2; Scale/3.00)",           new UserAgent(ClientPlatform.IOS, new Semver("3.9.0"), "(iPhone; iOS 12.2; Scale/3.00)") }
        };
    }
}
