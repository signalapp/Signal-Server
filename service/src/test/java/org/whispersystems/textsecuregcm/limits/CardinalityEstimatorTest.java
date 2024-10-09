/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import static org.assertj.core.api.Assertions.assertThat;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import java.time.Duration;

public class CardinalityEstimatorTest {

  @RegisterExtension
  private static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  @Test
  public void testAdd() throws Exception {
    final FaultTolerantRedisClusterClient redisCluster = REDIS_CLUSTER_EXTENSION.getRedisCluster();
    final CardinalityEstimator estimator = new CardinalityEstimator(redisCluster, "test", Duration.ofSeconds(1));

    estimator.add("1");

    long count = redisCluster.withCluster(conn -> conn.sync().pfcount("cardinality_estimator::test"));
    assertThat(count).isEqualTo(1).isEqualTo(estimator.estimate());

    estimator.add("2");
    count = redisCluster.withCluster(conn -> conn.sync().pfcount("cardinality_estimator::test"));
    assertThat(count).isEqualTo(2).isEqualTo(estimator.estimate());

    estimator.add("1");
    count = redisCluster.withCluster(conn -> conn.sync().pfcount("cardinality_estimator::test"));
    assertThat(count).isEqualTo(2).isEqualTo(estimator.estimate());
  }

  @Test
  @Timeout(5)
  public void testEventuallyExpires() throws InterruptedException {
    final FaultTolerantRedisClusterClient redisCluster = REDIS_CLUSTER_EXTENSION.getRedisCluster();
    final CardinalityEstimator estimator = new CardinalityEstimator(redisCluster, "test", Duration.ofMillis(100));
    estimator.add("1");
    long count;
    do {
      count = redisCluster.withCluster(conn -> conn.sync().pfcount("cardinality_estimator::test"));
      Thread.sleep(1);
    } while (count != 0);
  }

}
