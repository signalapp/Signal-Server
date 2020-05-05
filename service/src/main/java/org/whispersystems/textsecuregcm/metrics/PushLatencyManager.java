package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Measures and records the latency between sending a push notification to a device and that device draining its queue
 * of messages.
 */
public class PushLatencyManager {
    private static final Logger         logger         = LoggerFactory.getLogger(PushLatencyManager.class);
    private static final String         TIMER_NAME     = MetricRegistry.name(PushLatencyManager.class, "latency");
    private static final int            TTL            = (int)Duration.ofDays(3).toSeconds();

    @VisibleForTesting
    static final int                    QUEUE_SIZE     = 1_000;

    private final ReplicatedJedisPool   jedisPool;
    private final ThreadPoolExecutor    threadPoolExecutor;

    public PushLatencyManager(final ReplicatedJedisPool jedisPool) {
        this(jedisPool, new ThreadPoolExecutor( 1, 1, 0L, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(QUEUE_SIZE), new ThreadPoolExecutor.DiscardPolicy()));
    }

    @VisibleForTesting
    PushLatencyManager(final ReplicatedJedisPool jedisPool, final ThreadPoolExecutor threadPoolExecutor) {
        this.jedisPool          = jedisPool;
        this.threadPoolExecutor = threadPoolExecutor;

        Metrics.gaugeCollectionSize(MetricRegistry.name(getClass(), "queueDepth"), Collections.emptyList(), threadPoolExecutor.getQueue());
    }

    public void recordPushSent(final String accountNumber, final long deviceId) {
        recordPushSent(accountNumber, deviceId, System.currentTimeMillis());
    }

    @VisibleForTesting
    void recordPushSent(final String accountNumber, final long deviceId, final long currentTime) {
        final String key = getFirstUnacknowledgedPushKey(accountNumber, deviceId);

        threadPoolExecutor.execute(() -> {
            try (final Jedis jedis = jedisPool.getWriteResource()) {
                final Transaction transaction = jedis.multi();
                transaction.setnx(key, String.valueOf(currentTime));
                transaction.expire(key, TTL);
                transaction.exec();
            }
        });
    }

    public void recordQueueRead(final String accountNumber, final long deviceId, final String userAgent) {
        threadPoolExecutor.execute(() -> {
            final Optional<Long> maybeLatency = getLatencyAndClearTimestamp(accountNumber, deviceId, System.currentTimeMillis());

            if (maybeLatency.isPresent()) {
                Metrics.timer(TIMER_NAME, UserAgentTagUtil.getUserAgentTags(userAgent)).record(maybeLatency.get(), TimeUnit.MILLISECONDS);
            }
        });
    }

    @VisibleForTesting
    Optional<Long> getLatencyAndClearTimestamp(final String accountNumber, final long deviceId, final long currentTimeMillis) {
        final String key = getFirstUnacknowledgedPushKey(accountNumber, deviceId);

        try (final Jedis jedis = jedisPool.getWriteResource()) {
            final Transaction transaction = jedis.multi();
            transaction.get(key);
            transaction.del(key);

            final List<Response<?>> responses       = transaction.execGetResponse();
            final String            timestampString = (String)responses.get(0).get();

            if (timestampString != null) {
                try {
                    return Optional.of(currentTimeMillis - Long.parseLong(timestampString, 10));
                } catch (final NumberFormatException e) {
                    logger.warn("Failed to parse timestamp: {}", timestampString);
                }
            }

            return Optional.empty();
        }
    }

    private static String getFirstUnacknowledgedPushKey(final String accountNumber, final long deviceId) {
        return "push_latency::" + accountNumber + "::" + deviceId;
    }
}
