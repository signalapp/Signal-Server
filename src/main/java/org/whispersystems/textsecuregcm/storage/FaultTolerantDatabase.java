package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.SharedMetricRegistries;
import org.jdbi.v3.core.Jdbi;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.function.Consumer;
import java.util.function.Function;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;

public class FaultTolerantDatabase {

  private final Jdbi           database;
  private final CircuitBreaker circuitBreaker;

  public FaultTolerantDatabase(String name, Jdbi database, CircuitBreakerConfiguration circuitBreakerConfiguration) {
    this.database       = database;
    this.circuitBreaker = CircuitBreaker.of(name, circuitBreakerConfiguration.toCircuitBreakerConfig());

    CircuitBreakerUtil.registerMetrics(SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME),
                                       circuitBreaker,
                                       FaultTolerantDatabase.class);
  }

  public void use(Consumer<Jdbi> consumer) {
    this.circuitBreaker.executeRunnable(() -> consumer.accept(database));
  }

  public <T> T with(Function<Jdbi, T> consumer) {
    return this.circuitBreaker.executeSupplier(() -> consumer.apply(database));
  }

  public Jdbi getDatabase() {
    return database;
  }
}
