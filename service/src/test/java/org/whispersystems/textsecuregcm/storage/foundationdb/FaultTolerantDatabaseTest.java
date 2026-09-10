/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage.foundationdb;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.apple.foundationdb.Database;
import com.apple.foundationdb.FDBException;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.util.ResilienceUtil;

public class FaultTolerantDatabaseTest {
  private Database database;
  private FaultTolerantDatabase ftDatabase;

  @BeforeEach
  void setUp() {
    database = mock(Database.class);
  }

  @Test
  void circuitBreaker() throws InterruptedException {
    final String circuitBreakerConfigurationName = "testCircuitBreaker";
    final CircuitBreakerConfiguration circuitBreakerConfiguration = new CircuitBreakerConfiguration();

    circuitBreakerConfiguration.setSlidingWindowSize(2);
    circuitBreakerConfiguration.setSlidingWindowMinimumNumberOfCalls(2);
    circuitBreakerConfiguration.setPermittedNumberOfCallsInHalfOpenState(1);
    circuitBreakerConfiguration.setFailureRateThreshold(50);
    circuitBreakerConfiguration.setWaitDurationInOpenState(Duration.ofSeconds(1));

    ResilienceUtil.getCircuitBreakerRegistry()
        .addConfiguration(circuitBreakerConfigurationName, circuitBreakerConfiguration.toCircuitBreakerConfig());
    ftDatabase = new FaultTolerantDatabase(database, getClass().getSimpleName() + "testCircuitBreaker",
        circuitBreakerConfigurationName);

    when(database.read(any())).thenThrow(new FDBException("error", 1031));

    assertThrows(FDBException.class, () -> ftDatabase.read(t ->
        t.get("test_key".getBytes()), FaultTolerantDatabase.Context.TEST));

    assertThrows(FDBException.class, () -> ftDatabase.read(t ->
        t.get("test_key".getBytes()), FaultTolerantDatabase.Context.TEST));

    assertThrows(CallNotPermittedException.class, () -> ftDatabase.read(t ->
        t.get("test_key".getBytes()), FaultTolerantDatabase.Context.TEST));

    Thread.sleep(1001);

    assertThrows(FDBException.class, () -> ftDatabase.read(t ->
        t.get("test_key".getBytes()), FaultTolerantDatabase.Context.TEST));

    assertThrows(CallNotPermittedException.class, () -> ftDatabase.read(t ->
        t.get("test_key".getBytes()), FaultTolerantDatabase.Context.TEST));
  }
}
