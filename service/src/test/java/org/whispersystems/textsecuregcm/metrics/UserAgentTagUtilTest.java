/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.Tag;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class UserAgentTagUtilTest {

  @ParameterizedTest
  @MethodSource
  public void testGetUserAgentTags(final String userAgent, final List<Tag> expectedTags) {
    assertEquals(new HashSet<>(expectedTags),
        new HashSet<>(UserAgentTagUtil.getUserAgentTags(userAgent)));
  }

  private static List<Tag> platformVersionTags(String platform, String version) {
    return List.of(Tag.of(UserAgentTagUtil.PLATFORM_TAG, platform), Tag.of(UserAgentTagUtil.VERSION_TAG, version));
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> testGetUserAgentTags() {
    return Stream.of(
        Arguments.of("This is obviously not a reasonable User-Agent string.", UserAgentTagUtil.UNRECOGNIZED_TAGS),
        Arguments.of(null, UserAgentTagUtil.UNRECOGNIZED_TAGS),
        Arguments.of("Signal-Android 4.53.7 (Android 8.1)", platformVersionTags("android", "4.53.7")),
        Arguments.of("Signal Desktop 1.2.3", platformVersionTags("desktop", "1.2.3")),
        Arguments.of("Signal/3.9.0 (iPhone; iOS 12.2; Scale/3.00)", platformVersionTags("ios", "3.9.0")),
        Arguments.of("Signal-Android 1.2.3 (Android 8.1)", UserAgentTagUtil.UNRECOGNIZED_TAGS),
        Arguments.of("Signal Desktop 3.9.0", platformVersionTags("desktop", "3.9.0")),
        Arguments.of("Signal/4.53.7 (iPhone; iOS 12.2; Scale/3.00)", platformVersionTags("ios", "4.53.7")),
        Arguments.of("Signal-Android 4.68.3 (Android 9)", platformVersionTags("android", "4.68.3")),
        Arguments.of("Signal-Android 1.2.3 (Android 4.3)", UserAgentTagUtil.UNRECOGNIZED_TAGS),
        Arguments.of("Signal-Android 4.68.3.0-bobsbootlegclient", UserAgentTagUtil.UNRECOGNIZED_TAGS),
        Arguments.of("Signal Desktop 1.22.45-foo-0", UserAgentTagUtil.UNRECOGNIZED_TAGS),
        Arguments.of("Signal Desktop 1.34.5-beta.1-fakeclientemporium", UserAgentTagUtil.UNRECOGNIZED_TAGS),
        Arguments.of("Signal Desktop 1.32.0-beta.3", UserAgentTagUtil.UNRECOGNIZED_TAGS)
    );
  }

  @Test
  void testGetUserAgentTagsFlooded() {
    for (int i = 0; i < UserAgentTagUtil.MAX_VERSIONS; i++) {
      UserAgentTagUtil.getUserAgentTags(String.format("Signal-Android 4.0.%d (Android 8.1)", i));
    }

    assertEquals(UserAgentTagUtil.OVERFLOW_TAGS,
        UserAgentTagUtil.getUserAgentTags("Signal-Android 4.1.0 (Android 8.1)"));

    final List<Tag> tags = UserAgentTagUtil.getUserAgentTags("Signal-Android 4.0.0 (Android 8.1)");

    assertEquals(2, tags.size());
    assertTrue(tags.contains(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")));
    assertTrue(tags.contains(Tag.of(UserAgentTagUtil.VERSION_TAG, "4.0.0")));
  }

  @ParameterizedTest
  @MethodSource("argumentsForTestGetPlatformTag")
  public void testGetPlatformTag(final String userAgent, final Tag expectedTag) {
    assertEquals(expectedTag, UserAgentTagUtil.getPlatformTag(userAgent));
  }

  private static Stream<Arguments> argumentsForTestGetPlatformTag() {
    return Stream.of(
        Arguments.of("This is obviously not a reasonable User-Agent string.",
            Tag.of(UserAgentTagUtil.PLATFORM_TAG, "unrecognized")),
        Arguments.of(null, Tag.of(UserAgentTagUtil.PLATFORM_TAG, "unrecognized")),
        Arguments.of("Signal-Android 4.53.7 (Android 8.1)", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")),
        Arguments.of("Signal Desktop 1.2.3", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop")),
        Arguments.of("Signal/3.9.0 (iPhone; iOS 12.2; Scale/3.00)", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "ios")),
        Arguments.of("Signal-Android 1.2.3 (Android 8.1)", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")),
        Arguments.of("Signal Desktop 3.9.0", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop")),
        Arguments.of("Signal/4.53.7 (iPhone; iOS 12.2; Scale/3.00)", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "ios")),
        Arguments.of("Signal-Android 4.68.3 (Android 9)", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")),
        Arguments.of("Signal-Android 1.2.3 (Android 4.3)", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")),
        Arguments.of("Signal-Android 4.68.3.0-bobsbootlegclient", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")),
        Arguments.of("Signal Desktop 1.22.45-foo-0", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop")),
        Arguments.of("Signal Desktop 1.34.5-beta.1-fakeclientemporium",
            Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop")),
        Arguments.of("Signal Desktop 1.32.0-beta.3", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop"))
    );
  }
}
