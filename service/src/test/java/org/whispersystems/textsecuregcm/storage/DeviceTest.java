/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class DeviceTest {

  @ParameterizedTest
  @MethodSource
  void testIsEnabled(final boolean primary, final boolean fetchesMessages, final String apnId, final String gcmId,
      final Duration timeSinceLastSeen, final boolean expectEnabled) {

    final long lastSeen = System.currentTimeMillis() - timeSinceLastSeen.toMillis();

    final Device device = new Device();
    device.setId(primary ? Device.PRIMARY_ID : Device.PRIMARY_ID + 1);
    device.setFetchesMessages(fetchesMessages);
    device.setApnId(apnId);
    device.setGcmId(gcmId);
    device.setCreated(lastSeen);
    device.setLastSeen(lastSeen);

    assertEquals(expectEnabled, device.isEnabled());
  }

  private static Stream<Arguments> testIsEnabled() {
    return Stream.of(
        //           primary fetchesMessages apnId     gcmId     lastSeen             expectEnabled
        Arguments.of(true,   false,          null,     null,     Duration.ofDays(60), false),
        Arguments.of(true,   false,          null,     null,     Duration.ofDays(1),  false),
        Arguments.of(true,   false,          null,     "gcm-id", Duration.ofDays(60), true),
        Arguments.of(true,   false,          null,     "gcm-id", Duration.ofDays(1),  true),
        Arguments.of(true,   false,          "apn-id", null,     Duration.ofDays(60), true),
        Arguments.of(true,   false,          "apn-id", null,     Duration.ofDays(1),  true),
        Arguments.of(true,   true,           null,     null,     Duration.ofDays(60), true),
        Arguments.of(true,   true,           null,     null,     Duration.ofDays(1),  true),
        Arguments.of(false,  false,          null,     null,     Duration.ofDays(60), false),
        Arguments.of(false,  false,          null,     null,     Duration.ofDays(1),  false),
        Arguments.of(false,  false,          null,     "gcm-id", Duration.ofDays(60), false),
        Arguments.of(false,  false,          null,     "gcm-id", Duration.ofDays(1),  true),
        Arguments.of(false,  false,          "apn-id", null,     Duration.ofDays(60), false),
        Arguments.of(false,  false,          "apn-id", null,     Duration.ofDays(1),  true),
        Arguments.of(false,  true,           null,     null,     Duration.ofDays(60), false),
        Arguments.of(false,  true,           null,     null,     Duration.ofDays(1),  true)
    );
  }

  @ParameterizedTest
  @CsvSource({
      "true, P1D, false",
      "true, P30D, false",
      "true, P31D, false",
      "true, P180D, false",
      "true, P181D, true",
      "false, P1D, false",
      "false, P30D, false",
      "false, P31D, true",
      "false, P180D, true",
  })
  public void testIsExpired(final boolean primary, final Duration timeSinceLastSeen, final boolean expectExpired) {

    final long lastSeen = Instant.now()
        .minus(timeSinceLastSeen)
        // buffer for test runtime
        .plusSeconds(1)
        .toEpochMilli();

    final Device device = new Device();
    device.setId(primary ? Device.PRIMARY_ID : Device.PRIMARY_ID + 1);
    device.setCreated(lastSeen);
    device.setLastSeen(lastSeen);

    assertEquals(expectExpired, device.isExpired());
  }

}
