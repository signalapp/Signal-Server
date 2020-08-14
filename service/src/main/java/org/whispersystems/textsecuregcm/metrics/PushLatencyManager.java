package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import io.lettuce.core.SetArgs;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

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
    private static final String TIMER_NAME = MetricRegistry.name(PushLatencyManager.class, "latency");
    private static final int    TTL        = (int)Duration.ofDays(1).toSeconds();

    private final FaultTolerantRedisCluster redisCluster;

    private static final Logger log = LoggerFactory.getLogger(PushLatencyManager.class);

    public PushLatencyManager(final FaultTolerantRedisCluster redisCluster) {
        this.redisCluster = redisCluster;
    }

    public void recordPushSent(final UUID accountUuid, final long deviceId) {
        recordPushSent(accountUuid, deviceId, System.currentTimeMillis());
    }

    @VisibleForTesting
    void recordPushSent(final UUID accountUuid, final long deviceId, final long currentTime) {
        try {
            redisCluster.useCluster(connection ->
                    connection.sync().set(getFirstUnacknowledgedPushKey(accountUuid, deviceId), String.valueOf(currentTime), SetArgs.Builder.nx().ex(TTL)));
        } catch (final Exception e) {
            log.warn("Failed to record \"push notification sent\" timestamp", e);
        }
    }

    public void recordQueueRead(final UUID accountUuid, final long deviceId, final String userAgent) {
        final Optional<Long> maybeLatency = getLatencyAndClearTimestamp(accountUuid, deviceId, System.currentTimeMillis());

        if (maybeLatency.isPresent()) {
            Metrics.timer(TIMER_NAME, UserAgentTagUtil.getUserAgentTags(userAgent)).record(maybeLatency.get(), TimeUnit.MILLISECONDS);
        }
    }

    @VisibleForTesting
    Optional<Long> getLatencyAndClearTimestamp(final UUID accountUuid, final long deviceId, final long currentTimeMillis) {
        final String key = getFirstUnacknowledgedPushKey(accountUuid, deviceId);

        return redisCluster.withCluster(connection -> {
            final RedisAdvancedClusterCommands<String, String> commands = connection.sync();

            final String timestampString = commands.get(key);
            commands.del(key);

            return timestampString != null ? Optional.of(currentTimeMillis - Long.parseLong(timestampString, 10)) : Optional.empty();
        });
    }

    private static String getFirstUnacknowledgedPushKey(final UUID accountUuid, final long deviceId) {
        return "push_latency::" + accountUuid.toString() + "::" + deviceId;
    }
}
