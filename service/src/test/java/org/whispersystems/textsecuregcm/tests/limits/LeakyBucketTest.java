package org.whispersystems.textsecuregcm.tests.limits;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.whispersystems.textsecuregcm.limits.LeakyBucket;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LeakyBucketTest {

  @Test
  public void testFull() {
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
  public void testLapseRate() throws IOException {
    ObjectMapper mapper     = new ObjectMapper();
    String       serialized = "{\"bucketSize\":2,\"leakRatePerMillis\":8.333333333333334E-6,\"spaceRemaining\":0,\"lastUpdateTimeMillis\":" + (System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(2)) + "}";

    LeakyBucket leakyBucket = LeakyBucket.fromSerialized(mapper, serialized);
    assertTrue(leakyBucket.add(1));

    String      serializedAgain  = leakyBucket.serialize(mapper);
    LeakyBucket leakyBucketAgain = LeakyBucket.fromSerialized(mapper, serializedAgain);

    assertFalse(leakyBucketAgain.add(1));
  }

  @Test
  public void testLapseShort() throws Exception {
    ObjectMapper mapper     = new ObjectMapper();
    String       serialized = "{\"bucketSize\":2,\"leakRatePerMillis\":8.333333333333334E-6,\"spaceRemaining\":0,\"lastUpdateTimeMillis\":" + (System.currentTimeMillis() - TimeUnit.MINUTES.toMillis(1)) + "}";

    LeakyBucket leakyBucket = LeakyBucket.fromSerialized(mapper, serialized);
    assertFalse(leakyBucket.add(1));
  }
}
