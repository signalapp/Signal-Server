/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import io.lettuce.core.SetArgs;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.util.SystemMapper;

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
  private final ClientReleaseManager clientReleaseManager;

  private final Clock clock;

  public static final String TIMER_NAME = MetricRegistry.name(PushLatencyManager.class, "latency");
  private static final int TTL = (int) Duration.ofDays(1).toSeconds();

  private static final Logger log = LoggerFactory.getLogger(PushLatencyManager.class);

  @VisibleForTesting
  enum PushType {
    STANDARD,
    VOIP
  }

  record PushRecord(Instant timestamp, PushType pushType, Optional<Boolean> urgent) {
  }

  public PushLatencyManager(final FaultTolerantRedisCluster redisCluster,
      final ClientReleaseManager clientReleaseManager) {

    this(redisCluster, clientReleaseManager, Clock.systemUTC());
  }

  @VisibleForTesting
  PushLatencyManager(final FaultTolerantRedisCluster redisCluster,
      final ClientReleaseManager clientReleaseManager,
      final Clock clock) {

    this.redisCluster = redisCluster;
    this.clientReleaseManager = clientReleaseManager;
    this.clock = clock;
  }

  void recordPushSent(final UUID accountUuid, final byte deviceId, final boolean isVoip, final boolean isUrgent) {
    try {
      final String recordJson = SystemMapper.jsonMapper().writeValueAsString(
          new PushRecord(Instant.now(clock), isVoip ? PushType.VOIP : PushType.STANDARD, Optional.of(isUrgent)));

      redisCluster.useCluster(connection ->
          connection.async().set(getFirstUnacknowledgedPushKey(accountUuid, deviceId),
              recordJson,
              SetArgs.Builder.nx().ex(TTL)));
    } catch (final JsonProcessingException e) {
      // This should never happen
      log.error("Failed to write push latency record JSON", e);
    }
  }

  void recordQueueRead(final UUID accountUuid, final byte deviceId, final String userAgentString) {
    takePushRecord(accountUuid, deviceId).thenAccept(pushRecord -> {
      if (pushRecord != null) {
        final Duration latency = Duration.between(pushRecord.timestamp(), Instant.now());

        final List<Tag> tags = new ArrayList<>(3);

        tags.add(UserAgentTagUtil.getPlatformTag(userAgentString));
        tags.add(Tag.of("pushType", pushRecord.pushType().name().toLowerCase()));

        UserAgentTagUtil.getClientVersionTag(userAgentString, clientReleaseManager)
            .ifPresent(tags::add);

        pushRecord.urgent().ifPresent(urgent -> tags.add(Tag.of("urgent", String.valueOf(urgent))));

        Timer.builder(TIMER_NAME)
            .publishPercentileHistogram(true)
            .tags(tags)
            .register(Metrics.globalRegistry)
            .record(latency);
      }
    });
  }

  @VisibleForTesting
  CompletableFuture<PushRecord> takePushRecord(final UUID accountUuid, final byte deviceId) {
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

  private static String getFirstUnacknowledgedPushKey(final UUID accountUuid, final byte deviceId) {
    return "push_latency::v2::" + accountUuid.toString() + "::" + deviceId;
  }
}
