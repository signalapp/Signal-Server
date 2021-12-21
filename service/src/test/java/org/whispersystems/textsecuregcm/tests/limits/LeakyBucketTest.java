/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.limits;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.limits.LeakyBucket;

class LeakyBucketTest {

  @Test
  void testFull() {
    LeakyBucket leakyBucket = new LeakyBucket(2, 1.0 / 2.0);

    assertTrue(leakyBucket.add(1));
    assertTrue(leakyBucket.add(1));
    assertFalse(leakyBucket.add(1));

    leakyBucket = new LeakyBucket(2, 1.0 / 2.0);

    assertTrue(leakyBucket.add(2));
    assertFalse(leakyBucket.add(1));
    assertFalse(leakyBucket.add(2));
  }

  @Test
  void testLapseRate() throws IOException {
    ObjectMapper mapper     = new ObjectMapper();
    String       serialized = "{\"bucketSize\":2,\"leakRatePerMillis\":8.333333333333334E-6,\"spaceRemaining\":0,\"lastUpdateTimeMillis\":" + (System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(2)) + "}";

    LeakyBucket leakyBucket = LeakyBucket.fromSerialized(mapper, serialized);
    assertTrue(leakyBucket.add(1));

    String      serializedAgain  = leakyBucket.serialize(mapper);
    LeakyBucket leakyBucketAgain = LeakyBucket.fromSerialized(mapper, serializedAgain);

    assertFalse(leakyBucketAgain.add(1));
  }

  @Test
  void testLapseShort() throws Exception {
    ObjectMapper mapper     = new ObjectMapper();
    String       serialized = "{\"bucketSize\":2,\"leakRatePerMillis\":8.333333333333334E-6,\"spaceRemaining\":0,\"lastUpdateTimeMillis\":" + (System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1)) + "}";

    LeakyBucket leakyBucket = LeakyBucket.fromSerialized(mapper, serialized);
    assertFalse(leakyBucket.add(1));
  }

  @Test
  void testGetTimeUntilSpaceAvailable() throws Exception {
    ObjectMapper mapper = new ObjectMapper();

    {
      String serialized = "{\"bucketSize\":2,\"leakRatePerMillis\":8.333333333333334E-6,\"spaceRemaining\":2,\"lastUpdateTimeMillis\":" + (System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1)) + "}";

      LeakyBucket leakyBucket = LeakyBucket.fromSerialized(mapper, serialized);

      assertEquals(Duration.ZERO, leakyBucket.getTimeUntilSpaceAvailable(1));
      assertThrows(IllegalArgumentException.class, () -> leakyBucket.getTimeUntilSpaceAvailable(5000));
    }

    {
      String serialized = "{\"bucketSize\":2,\"leakRatePerMillis\":8.333333333333334E-6,\"spaceRemaining\":0,\"lastUpdateTimeMillis\":" + (System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1)) + "}";

      LeakyBucket leakyBucket = LeakyBucket.fromSerialized(mapper, serialized);

      Duration timeUntilSpaceAvailable = leakyBucket.getTimeUntilSpaceAvailable(1);

      // TODO Refactor LeakyBucket to be more test-friendly and accept a Clock
      assertTrue(timeUntilSpaceAvailable.compareTo(Duration.ofMillis(119_000)) > 0);
      assertTrue(timeUntilSpaceAvailable.compareTo(Duration.ofMinutes(2)) <= 0);
    }
  }
}
