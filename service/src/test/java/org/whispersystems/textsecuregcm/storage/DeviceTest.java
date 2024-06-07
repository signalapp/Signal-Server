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
  void testHasMessageDeliveryChannel(final boolean fetchesMessages, final String apnId, final String gcmId, final boolean expectEnabled) {

    final Device device = new Device();
    device.setFetchesMessages(fetchesMessages);
    device.setApnId(apnId);
    device.setGcmId(gcmId);

    assertEquals(expectEnabled, device.hasMessageDeliveryChannel());
  }

  private static Stream<Arguments> testHasMessageDeliveryChannel() {
    return Stream.of(
        Arguments.of(false, null, null, false),
        Arguments.of(false, null, "gcm-id", true),
        Arguments.of(false, "apn-id", null, true),
        Arguments.of(true, null, null, true)
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
