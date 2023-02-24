/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.vdurmont.semver4j.Semver;
import io.lettuce.core.SetArgs;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

/**
 * Measures and records the latency between sending a push notification to a device and that device draining its queue
 * of messages.
 * <p/>
 * When the server sends a push notification to a device, the push latency manager creates a Redis key/value pair
 * mapping the current timestamp to the given device if such a mapping doesn't already exist. When a client connects and
 * clears its message queue, the push latency manager gets and clears the time of the initial push notification to that
 * device and records the time elapsed since the push notification timestamp as a latency observation.
 */
public class PushLatencyManager {

  private final FaultTolerantRedisCluster redisCluster;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  private final Clock clock;

  private static final String TIMER_NAME = MetricRegistry.name(PushLatencyManager.class, "latency");
  private static final int TTL = (int) Duration.ofDays(1).toSeconds();

  private static final Logger log = LoggerFactory.getLogger(PushLatencyManager.class);

  @VisibleForTesting
  enum PushType {
    STANDARD,
    VOIP
  }

  @VisibleForTesting
  static class PushRecord {
    private final Instant timestamp;

    @Nullable
    private final PushType pushType;

    @JsonCreator
    PushRecord(@JsonProperty("timestamp") final Instant timestamp,
        @JsonProperty("pushType") @Nullable final PushType pushType) {

      this.timestamp = timestamp;
      this.pushType = pushType;
    }

    public Instant getTimestamp() {
      return timestamp;
    }

    @Nullable
    public PushType getPushType() {
      return pushType;
    }
  }

  public PushLatencyManager(final FaultTolerantRedisCluster redisCluster,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {

    this(redisCluster, dynamicConfigurationManager, Clock.systemUTC());
  }

  @VisibleForTesting
  PushLatencyManager(final FaultTolerantRedisCluster redisCluster,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final Clock clock) {

    this.redisCluster = redisCluster;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.clock = clock;
  }

  void recordPushSent(final UUID accountUuid, final long deviceId, final boolean isVoip) {
    try {
      final String recordJson = SystemMapper.jsonMapper().writeValueAsString(
          new PushRecord(Instant.now(clock), isVoip ? PushType.VOIP : PushType.STANDARD));

      redisCluster.useCluster(connection ->
          connection.async().set(getFirstUnacknowledgedPushKey(accountUuid, deviceId),
              recordJson,
              SetArgs.Builder.nx().ex(TTL)));
    } catch (final JsonProcessingException e) {
      // This should never happen
      log.error("Failed to write push latency record JSON", e);
    }
  }

  void recordQueueRead(final UUID accountUuid, final long deviceId, final String userAgentString) {
    takePushRecord(accountUuid, deviceId).thenAccept(pushRecord -> {
      if (pushRecord != null) {
        final Duration latency = Duration.between(pushRecord.getTimestamp(), Instant.now());

        final List<Tag> tags = new ArrayList<>(2);

        tags.add(UserAgentTagUtil.getPlatformTag(userAgentString));

        if (pushRecord.getPushType() != null) {
          tags.add(Tag.of("pushType", pushRecord.getPushType().name().toLowerCase()));
        }

        try {
          final UserAgent userAgent = UserAgentUtil.parseUserAgentString(userAgentString);

          final Set<Semver> instrumentedVersions =
              dynamicConfigurationManager.getConfiguration().getPushLatencyConfiguration().getInstrumentedVersions()
                  .getOrDefault(userAgent.getPlatform(), Collections.emptySet());

          if (instrumentedVersions.contains(userAgent.getVersion())) {
            tags.add(Tag.of("clientVersion", userAgent.getVersion().toString()));
          }
        } catch (UnrecognizedUserAgentException ignored) {
        }

        Metrics.timer(TIMER_NAME, tags).record(latency);
      }
    });
  }

  @VisibleForTesting
  CompletableFuture<PushRecord> takePushRecord(final UUID accountUuid, final long deviceId) {
    final String key = getFirstUnacknowledgedPushKey(accountUuid, deviceId);

    return redisCluster.withCluster(connection -> {
      final CompletableFuture<PushRecord> getFuture = connection.async().get(key).toCompletableFuture()
          .thenApply(recordJson -> {
            if (StringUtils.isNotEmpty(recordJson)) {
              try {
                return SystemMapper.jsonMapper().readValue(recordJson, PushRecord.class);
              } catch (JsonProcessingException e) {
                return null;
              }
            } else {
              return null;
            }
          });

      getFuture.whenComplete((record, cause) -> {
        if (cause == null) {
          connection.async().del(key);
        }
      });

      return getFuture;
    });
  }

  private static String getFirstUnacknowledgedPushKey(final UUID accountUuid, final long deviceId) {
    return "push_latency::v2::" + accountUuid.toString() + "::" + deviceId;
  }
}
