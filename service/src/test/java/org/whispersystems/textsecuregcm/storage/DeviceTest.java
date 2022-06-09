/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.time.Duration;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.entities.SignedPreKey;

class DeviceTest {

  @ParameterizedTest
  @MethodSource
  void testIsEnabled(final boolean master, final boolean fetchesMessages, final String apnId, final String gcmId,
      final SignedPreKey signedPreKey, final Duration timeSinceLastSeen, final boolean expectEnabled) {
    final long lastSeen = System.currentTimeMillis() - timeSinceLastSeen.toMillis();
    final Device device = new Device(master ? 1 : 2, "test", "auth-token", "salt", gcmId, apnId, null, fetchesMessages,
        1, signedPreKey, lastSeen, lastSeen, "user-agent", 0, null);

    assertEquals(expectEnabled, device.isEnabled());
  }

  private static Stream<Arguments> testIsEnabled() {
    return Stream.of(
        //             master fetchesMessages apnId     gcmId     signedPreKey              lastSeen             expectEnabled
        Arguments.of(true, false, null, null, null, Duration.ofDays(60), false),
        Arguments.of(true, false, null, null, null, Duration.ofDays(1), false),
        Arguments.of(true, false, null, null, mock(SignedPreKey.class), Duration.ofDays(60), false),
        Arguments.of(true, false, null, null, mock(SignedPreKey.class), Duration.ofDays(1), false),
        Arguments.of(true, false, null, "gcm-id", null, Duration.ofDays(60), false),
        Arguments.of(true, false, null, "gcm-id", null, Duration.ofDays(1), false),
        Arguments.of(true, false, null, "gcm-id", mock(SignedPreKey.class), Duration.ofDays(60), true),
        Arguments.of(true, false, null, "gcm-id", mock(SignedPreKey.class), Duration.ofDays(1), true),
        Arguments.of(true, false, "apn-id", null, null, Duration.ofDays(60), false),
        Arguments.of(true, false, "apn-id", null, null, Duration.ofDays(1), false),
        Arguments.of(true, false, "apn-id", null, mock(SignedPreKey.class), Duration.ofDays(60), true),
        Arguments.of(true, false, "apn-id", null, mock(SignedPreKey.class), Duration.ofDays(1), true),
        Arguments.of(true, true, null, null, null, Duration.ofDays(60), false),
        Arguments.of(true, true, null, null, null, Duration.ofDays(1), false),
        Arguments.of(true, true, null, null, mock(SignedPreKey.class), Duration.ofDays(60), true),
        Arguments.of(true, true, null, null, mock(SignedPreKey.class), Duration.ofDays(1), true),
        Arguments.of(false, false, null, null, null, Duration.ofDays(60), false),
        Arguments.of(false, false, null, null, null, Duration.ofDays(1), false),
        Arguments.of(false, false, null, null, mock(SignedPreKey.class), Duration.ofDays(60), false),
        Arguments.of(false, false, null, null, mock(SignedPreKey.class), Duration.ofDays(1), false),
        Arguments.of(false, false, null, "gcm-id", null, Duration.ofDays(60), false),
        Arguments.of(false, false, null, "gcm-id", null, Duration.ofDays(1), false),
        Arguments.of(false, false, null, "gcm-id", mock(SignedPreKey.class), Duration.ofDays(60), false),
        Arguments.of(false, false, null, "gcm-id", mock(SignedPreKey.class), Duration.ofDays(1), true),
        Arguments.of(false, false, "apn-id", null, null, Duration.ofDays(60), false),
        Arguments.of(false, false, "apn-id", null, null, Duration.ofDays(1), false),
        Arguments.of(false, false, "apn-id", null, mock(SignedPreKey.class), Duration.ofDays(60), false),
        Arguments.of(false, false, "apn-id", null, mock(SignedPreKey.class), Duration.ofDays(1), true),
        Arguments.of(false, true, null, null, null, Duration.ofDays(60), false),
        Arguments.of(false, true, null, null, null, Duration.ofDays(1), false),
        Arguments.of(false, true, null, null, mock(SignedPreKey.class), Duration.ofDays(60), false),
        Arguments.of(false, true, null, null, mock(SignedPreKey.class), Duration.ofDays(1), true)
    );
  }

  @ParameterizedTest
  @MethodSource("argumentsForTestIsGroupsV2Supported")
  void testIsGroupsV2Supported(final boolean master, final String apnId, final boolean gv2Capability,
      final boolean gv2_2Capability, final boolean gv2_3Capability, final boolean expectGv2Supported) {
    final Device.DeviceCapabilities capabilities = new Device.DeviceCapabilities(gv2Capability, gv2_2Capability,
        gv2_3Capability, false, false, false,
        false, false, false, false, false, false);
    final Device device = new Device(master ? 1 : 2, "test", "auth-token", "salt",
        null, apnId, null, false, 1, null, 0, 0, "user-agent", 0, capabilities);

    assertEquals(expectGv2Supported, device.isGroupsV2Supported());
  }

  private static Stream<Arguments> argumentsForTestIsGroupsV2Supported() {
    return Stream.of(
        //             master apnId     gv2    gv2-2  gv2-3  capable

        // Android master
        Arguments.of(true, null, false, false, false, false),
        Arguments.of(true, null, true, false, false, false),
        Arguments.of(true, null, false, true, false, false),
        Arguments.of(true, null, true, true, false, false),
        Arguments.of(true, null, false, false, true, true),
        Arguments.of(true, null, true, false, true, true),
        Arguments.of(true, null, false, true, true, true),
        Arguments.of(true, null, true, true, true, true),

        // iOS master
        Arguments.of(true, "apn-id", false, false, false, false),
        Arguments.of(true, "apn-id", true, false, false, false),
        Arguments.of(true, "apn-id", false, true, false, true),
        Arguments.of(true, "apn-id", true, true, false, true),
        Arguments.of(true, "apn-id", false, false, true, true),
        Arguments.of(true, "apn-id", true, false, true, true),
        Arguments.of(true, "apn-id", false, true, true, true),
        Arguments.of(true, "apn-id", true, true, true, true),

        // iOS linked
        Arguments.of(false, "apn-id", false, false, false, false),
        Arguments.of(false, "apn-id", true, false, false, false),
        Arguments.of(false, "apn-id", false, true, false, true),
        Arguments.of(false, "apn-id", true, true, false, true),
        Arguments.of(false, "apn-id", false, false, true, true),
        Arguments.of(false, "apn-id", true, false, true, true),
        Arguments.of(false, "apn-id", false, true, true, true),
        Arguments.of(false, "apn-id", true, true, true, true),

        // desktop linked
        Arguments.of(false, null, false, false, false, false),
        Arguments.of(false, null, true, false, false, false),
        Arguments.of(false, null, false, true, false, false),
        Arguments.of(false, null, true, true, false, false),
        Arguments.of(false, null, false, false, true, true),
        Arguments.of(false, null, true, false, true, true),
        Arguments.of(false, null, false, true, true, true),
        Arguments.of(false, null, true, true, true, true)
    );
  }
}
