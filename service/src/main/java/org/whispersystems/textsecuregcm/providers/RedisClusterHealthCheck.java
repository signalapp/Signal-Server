/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.providers;

import com.codahale.metrics.health.HealthCheck;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

import java.util.concurrent.CompletableFuture;

public class RedisClusterHealthCheck extends HealthCheck {

    private final FaultTolerantRedisCluster redisCluster;

    public RedisClusterHealthCheck(final FaultTolerantRedisCluster redisCluster) {
        this.redisCluster = redisCluster;
    }

    @Override
    protected Result check() {
        redisCluster.withCluster(connection -> connection.sync().masters().commands().ping());
        return Result.healthy();
    }
}
