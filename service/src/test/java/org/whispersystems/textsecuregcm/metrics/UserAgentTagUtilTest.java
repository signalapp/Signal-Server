/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.vdurmont.semver4j.Semver;
import io.micrometer.core.instrument.Tag;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
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
        Arguments.of("Signal-Desktop/1.32.0-beta.3", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop")),
        Arguments.of(UserAgentTagUtil.SERVER_UA, Tag.of(UserAgentTagUtil.PLATFORM_TAG, "server")),
        Arguments.of("Signal-Server/1.2.3 (" + UUID.randomUUID() + ")", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "unrecognized"))
    );
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void getClientVersionTag(final String userAgent, final boolean isVersionLive, final Optional<Tag> expectedTag) {
    final ClientReleaseManager clientReleaseManager = mock(ClientReleaseManager.class);
    when(clientReleaseManager.isVersionActive(any(), any())).thenReturn(isVersionLive);

    assertEquals(expectedTag, UserAgentTagUtil.getClientVersionTag(userAgent, clientReleaseManager));
  }

  private static Stream<Arguments> getClientVersionTag() {
    return Stream.of(
        Arguments.of("Signal-Android/1.2.3 (Android 9)",
            true,
            Optional.of(Tag.of(UserAgentTagUtil.VERSION_TAG, "1.2.3"))),

        Arguments.of("Signal-Android/1.2.3 (Android 9)",
            false,
            Optional.empty())
    );
  }
}
