/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import java.time.Duration;
import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.Constants;

public class AbusiveHostRules {

  private static final String KEY_PREFIX = "abusive_hosts::";
  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Timer getTimer = metricRegistry.timer(name(AbusiveHostRules.class, "get"));
  private final Timer insertTimer = metricRegistry.timer(name(AbusiveHostRules.class, "setBlockedHost"));

  private final FaultTolerantRedisCluster redisCluster;
  private final DynamicConfigurationManager<DynamicConfiguration> configurationManager;

  public AbusiveHostRules(FaultTolerantRedisCluster redisCluster, final DynamicConfigurationManager<DynamicConfiguration> configurationManager) {
    this.redisCluster = redisCluster;
    this.configurationManager = configurationManager;
  }

  public boolean isBlocked(String host) {
    try (Timer.Context timer = getTimer.time()) {
      return this.redisCluster.withCluster(connection -> connection.sync().exists(prefix(host))) > 0;
    }
  }

  public void setBlockedHost(String host) {
    Duration expireTime = configurationManager.getConfiguration().getAbusiveHostRules().getExpirationTime();
    try (Timer.Context timer = insertTimer.time()) {
      this.redisCluster.useCluster(connection -> connection.sync().setex(prefix(host), expireTime.toSeconds(), "1"));
    }
  }

  @VisibleForTesting
  public static String prefix(String keyName) {
    return KEY_PREFIX + keyName;
  }

}
