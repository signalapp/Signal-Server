/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.vdurmont.semver4j.Semver;
import io.micrometer.core.instrument.Tag;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

class UserAgentTagUtilTest {

  @ParameterizedTest
  @MethodSource
  void getPlatformTag(final String userAgent, final Tag expectedTag) {
    assertEquals(expectedTag, UserAgentTagUtil.getPlatformTag(userAgent));
  }

  private static Stream<Arguments> getPlatformTag() {
    return Stream.of(
        Arguments.of("This is obviously not a reasonable User-Agent string.", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "unrecognized")),
        Arguments.of(null, Tag.of(UserAgentTagUtil.PLATFORM_TAG, "unrecognized")),
        Arguments.of("Signal-Android/4.53.7 (Android 8.1)", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")),
        Arguments.of("Signal-Desktop/1.2.3", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop")),
        Arguments.of("Signal-iOS/3.9.0 (iPhone; iOS 12.2; Scale/3.00)", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "ios")),
        Arguments.of("Signal-Android/1.2.3 (Android 8.1)", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")),
        Arguments.of("Signal-Desktop/3.9.0", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop")),
        Arguments.of("Signal-iOS/4.53.7 (iPhone; iOS 12.2; Scale/3.00)", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "ios")),
        Arguments.of("Signal-Android/4.68.3 (Android 9)", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")),
        Arguments.of("Signal-Android/1.2.3 (Android 4.3)", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")),
        Arguments.of("Signal-Android/4.68.3.0-bobsbootlegclient", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")),
        Arguments.of("Signal-Desktop/1.22.45-foo-0", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop")),
        Arguments.of("Signal-Desktop/1.34.5-beta.1-fakeclientemporium", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop")),
        Arguments.of("Signal-Desktop/1.32.0-beta.3", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop"))
    );
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void getClientVersionTag(final String userAgent, final Map<ClientPlatform, Set<Semver>> taggedVersions, final Optional<Tag> expectedTag) {
    assertEquals(expectedTag, UserAgentTagUtil.getClientVersionTag(userAgent, taggedVersions));
  }

  private static Stream<Arguments> getClientVersionTag() {
    return Stream.of(
        Arguments.of("Signal-Android/1.2.3 (Android 9)",
            Map.of(ClientPlatform.ANDROID, Set.of(new Semver("1.2.3"))),
            Optional.of(Tag.of(UserAgentTagUtil.VERSION_TAG, "1.2.3"))),

        Arguments.of("Signal-Android/1.2.3 (Android 9)",
            Collections.emptyMap(),
            Optional.empty()),

        Arguments.of("Signal-Android/1.2.3.0-bobsbootlegclient",
            Map.of(ClientPlatform.ANDROID, Set.of(new Semver("1.2.3"))),
            Optional.empty()),

        Arguments.of("Signal-Desktop/1.2.3",
            Map.of(ClientPlatform.ANDROID, Set.of(new Semver("1.2.3"))),
            Optional.empty())
    );
  }
}
