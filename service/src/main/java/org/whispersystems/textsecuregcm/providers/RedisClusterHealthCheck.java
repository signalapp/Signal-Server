/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.providers;

import com.codahale.metrics.health.HealthCheck;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisClusterClient;

public class RedisClusterHealthCheck extends HealthCheck {

    private final FaultTolerantRedisClusterClient redisCluster;

    public RedisClusterHealthCheck(final FaultTolerantRedisClusterClient redisCluster) {
        this.redisCluster = redisCluster;
    }

    @Override
    protected Result check() {
        redisCluster.withCluster(connection -> connection.sync().upstream().commands().ping());
        return Result.healthy();
    }
}
