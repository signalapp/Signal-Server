package org.whispersystems.textsecuregcm.redis;

import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.lettuce.core.RedisCommandTimeoutException;
import io.lettuce.core.RedisException;
import io.lettuce.core.resource.ClientResources;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

// ThreadMode.SEPARATE_THREAD protects against hangs in the remote Redis calls, as this mode allows the test code to be
// preempted by the timeout check
@Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class FaultTolerantRedisClientTest {

  private static final Duration TIMEOUT = Duration.ofMillis(50);

  private static final RetryConfiguration RETRY_CONFIGURATION = new RetryConfiguration();

  static {
    RETRY_CONFIGURATION.setMaxAttempts(1);
    RETRY_CONFIGURATION.setWaitDuration(50);
  }

  @RegisterExtension
  static final RedisServerExtension REDIS_SERVER_EXTENSION = RedisServerExtension.builder().build();

  private FaultTolerantRedisClient faultTolerantRedisClient;

  private static FaultTolerantRedisClient buildRedisClient(
      @Nullable final CircuitBreakerConfiguration circuitBreakerConfiguration,
      final ClientResources.Builder clientResourcesBuilder) {

    return new FaultTolerantRedisClient("test", clientResourcesBuilder,
        RedisServerExtension.getRedisURI(), TIMEOUT,
        Optional.ofNullable(circuitBreakerConfiguration).orElseGet(CircuitBreakerConfiguration::new),
        RETRY_CONFIGURATION);
  }

  @AfterEach
  void tearDown() {
    faultTolerantRedisClient.shutdown();
  }

  @Test
  void testTimeout() {
    faultTolerantRedisClient = buildRedisClient(null, ClientResources.builder());

    final ExecutionException asyncException = assertThrows(ExecutionException.class,
        () -> faultTolerantRedisClient.withConnection(connection -> connection.async().blpop(2 * TIMEOUT.toMillis() / 1000d, "key"))
            .get());

    assertInstanceOf(RedisCommandTimeoutException.class, asyncException.getCause());

    assertThrows(RedisCommandTimeoutException.class,
        () -> faultTolerantRedisClient.withConnection(connection -> connection.sync().blpop(2 * TIMEOUT.toMillis() / 1000d, "key")));
  }

  @Test
  void testTimeoutCircuitBreaker() throws Exception {
    // because we’re using a single key, and blpop involves *Redis* also blocking, the breaker wait duration must be
    // longer than the sum of the remote timeouts
    final Duration breakerWaitDuration = TIMEOUT.multipliedBy(5);

    final CircuitBreakerConfiguration circuitBreakerConfig = new CircuitBreakerConfiguration();
    circuitBreakerConfig.setFailureRateThreshold(1);
    circuitBreakerConfig.setSlidingWindowMinimumNumberOfCalls(1);
    circuitBreakerConfig.setSlidingWindowSize(1);
    circuitBreakerConfig.setWaitDurationInOpenState(breakerWaitDuration);

    faultTolerantRedisClient = buildRedisClient(circuitBreakerConfig, ClientResources.builder());

    final String key = "key";

    // the first call should time out and open the breaker
    assertThrows(RedisCommandTimeoutException.class,
        () -> faultTolerantRedisClient.withConnection(connection -> connection.sync().blpop(2 * TIMEOUT.toMillis() / 1000d, key)));

    // the second call gets blocked by the breaker
    final RedisException e = assertThrows(RedisException.class,
        () -> faultTolerantRedisClient.withConnection(connection -> connection.sync().blpop(2 * TIMEOUT.toMillis() / 1000d, key)));
    assertInstanceOf(CallNotPermittedException.class, e.getCause());

    // wait for breaker to be half-open
    Thread.sleep(breakerWaitDuration.toMillis() * 2);

    assertEquals(0, (Long) faultTolerantRedisClient.withConnection(connection -> connection.sync().llen(key)));
  }
}
