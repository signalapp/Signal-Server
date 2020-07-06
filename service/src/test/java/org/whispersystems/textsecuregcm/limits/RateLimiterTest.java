package org.whispersystems.textsecuregcm.limits;

import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.AbstractRedisTest;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class RateLimiterTest extends AbstractRedisTest {

    private static final long   NOW_MILLIS = System.currentTimeMillis();
    private static final String KEY        = "key";

    @FunctionalInterface
    private interface RateLimitedTask {
        void run() throws RateLimitExceededException;
    }

    @Before
    public void clearCache() {
        try (final Jedis jedis = getReplicatedJedisPool().getWriteResource()) {
            jedis.flushAll();
        }
    }

    @Test
    public void validate() throws RateLimitExceededException, IOException {
        final RateLimiter rateLimiter = buildRateLimiter(2, 0.5);

        rateLimiter.validate(KEY, 1, NOW_MILLIS);
        rateLimiter.validate(KEY, 1, NOW_MILLIS);
        assertRateLimitExceeded(() -> rateLimiter.validate(KEY, 1, NOW_MILLIS));
    }

    @Test
    public void validateWithAmount() throws RateLimitExceededException, IOException {
        final RateLimiter rateLimiter = buildRateLimiter(2, 0.5);

        rateLimiter.validate(KEY, 2, NOW_MILLIS);
        assertRateLimitExceeded(() -> rateLimiter.validate(KEY, 1, NOW_MILLIS));
    }

    @Test
    public void testLapseRate() throws RateLimitExceededException, IOException {
        final RateLimiter rateLimiter     = buildRateLimiter(2, 8.333333333333334E-6);
        final String      leakyBucketJson = "{\"bucketSize\":2,\"leakRatePerMillis\":8.333333333333334E-6,\"spaceRemaining\":0,\"lastUpdateTimeMillis\":" + (NOW_MILLIS - TimeUnit.MINUTES.toMillis(2)) + "}";

        try (final Jedis jedis = getReplicatedJedisPool().getWriteResource()) {
            jedis.set(rateLimiter.getBucketName(KEY), leakyBucketJson);
        }

        rateLimiter.validate(KEY, 1, NOW_MILLIS);
        assertRateLimitExceeded(() -> rateLimiter.validate(KEY, 1, NOW_MILLIS));
    }

    @Test
    public void testLapseShort() throws IOException {
        final RateLimiter rateLimiter     = buildRateLimiter(2, 8.333333333333334E-6);
        final String      leakyBucketJson = "{\"bucketSize\":2,\"leakRatePerMillis\":8.333333333333334E-6,\"spaceRemaining\":0,\"lastUpdateTimeMillis\":" + (NOW_MILLIS - TimeUnit.MINUTES.toMillis(1)) + "}";

        try (final Jedis jedis = getReplicatedJedisPool().getWriteResource()) {
            jedis.set(rateLimiter.getBucketName(KEY), leakyBucketJson);
        }

        assertRateLimitExceeded(() -> rateLimiter.validate(KEY, 1, NOW_MILLIS));
    }

    private void assertRateLimitExceeded(final RateLimitedTask task) {
        try {
            task.run();
            fail("Expected RateLimitExceededException");
        } catch (final RateLimitExceededException ignored) {
        }
    }

    @SuppressWarnings("SameParameterValue")
    private RateLimiter buildRateLimiter(final int bucketSize, final double leakRatePerMilli) throws IOException {
        final double leakRatePerMinute = leakRatePerMilli * 60_000d;
        return new RateLimiter(getReplicatedJedisPool(), mock(FaultTolerantRedisCluster.class), KEY, bucketSize, leakRatePerMinute);
    }
}
