package org.whispersystems.textsecuregcm.limits;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.storage.Device;

class MessageDeliveryLoopMonitorTest {

  private RedisMessageDeliveryLoopMonitor messageDeliveryLoopMonitor;

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @BeforeEach
  void setUp() {
    messageDeliveryLoopMonitor = new RedisMessageDeliveryLoopMonitor(REDIS_CLUSTER_EXTENSION.getRedisCluster());
  }

  @Test
  void incrementDeliveryAttemptCount() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    assertEquals(1, messageDeliveryLoopMonitor.incrementDeliveryAttemptCount(accountIdentifier, deviceId, UUID.randomUUID()).join());
    assertEquals(1, messageDeliveryLoopMonitor.incrementDeliveryAttemptCount(accountIdentifier, deviceId, UUID.randomUUID()).join());

    final UUID repeatedDeliveryGuid = UUID.randomUUID();

    for (int i = 1; i < 10; i++) {
      assertEquals(i, messageDeliveryLoopMonitor.incrementDeliveryAttemptCount(accountIdentifier, deviceId, repeatedDeliveryGuid).join());
    }
  }
}
