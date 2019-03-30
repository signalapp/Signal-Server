package org.whispersystems.textsecuregcm.redis;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.util.Constants;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static com.codahale.metrics.MetricRegistry.name;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

public class ReplicatedJedisPool {

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Logger         logger         = LoggerFactory.getLogger(ReplicatedJedisPool.class);
  private final AtomicInteger  replicaIndex   = new AtomicInteger(0);

  private final Supplier<Jedis>            master;
  private final ArrayList<Supplier<Jedis>> replicas;

  public ReplicatedJedisPool(String name,
                             JedisPool master,
                             List<JedisPool> replicas,
                             CircuitBreakerConfiguration circuitBreakerConfiguration)
  {
    if (replicas.size() < 1) throw new IllegalArgumentException("There must be at least one replica");


    CircuitBreakerConfig config = CircuitBreakerConfig.custom()
                                                      .failureRateThreshold(circuitBreakerConfiguration.getFailureRateThreshold())
                                                      .ringBufferSizeInHalfOpenState(circuitBreakerConfiguration.getRingBufferSizeInHalfOpenState())
                                                      .waitDurationInOpenState(Duration.ofSeconds(circuitBreakerConfiguration.getWaitDurationInOpenStateInSeconds()))
                                                      .ringBufferSizeInClosedState(circuitBreakerConfiguration.getRingBufferSizeInClosedState())
                                                      .build();

    CircuitBreaker masterBreaker = CircuitBreaker.of(String.format("%s-master", name), config);
    registerMetrics(masterBreaker);

    this.master   = CircuitBreaker.decorateSupplier(masterBreaker, master::getResource);
    this.replicas = new ArrayList<>(replicas.size());

    for (int i=0;i<replicas.size();i++) {
      JedisPool      replica      = replicas.get(i);
      CircuitBreaker slaveBreaker = CircuitBreaker.of(String.format("%s-slave-%d", name, i), config);

      registerMetrics(slaveBreaker);
      this.replicas.add(CircuitBreaker.decorateSupplier(slaveBreaker, replica::getResource));
    }
  }

  public Jedis getWriteResource() {
    return master.get();
  }

  public Jedis getReadResource() {
    int failureCount = 0;

    while (failureCount < replicas.size()) {
      try {
        return replicas.get(replicaIndex.getAndIncrement() % replicas.size()).get();
      } catch (RuntimeException e) {
        logger.error("Failure obtaining read replica pool", e);
      }

      failureCount++;
    }

    throw new JedisException("All read replica pools failed!");
  }

  private void registerMetrics(CircuitBreaker circuitBreaker) {
    Meter successMeter     = metricRegistry.meter(name(ReplicatedJedisPool.class, circuitBreaker.getName(), "success"    ));
    Meter failureMeter     = metricRegistry.meter(name(ReplicatedJedisPool.class, circuitBreaker.getName(), "failure"    ));
    Meter unpermittedMeter = metricRegistry.meter(name(ReplicatedJedisPool.class, circuitBreaker.getName(), "unpermitted"));

    metricRegistry.gauge(name(ReplicatedJedisPool.class, circuitBreaker.getName(), "state"), () -> ()-> circuitBreaker.getState().getOrder());

    circuitBreaker.getEventPublisher().onSuccess(event -> successMeter.mark());
    circuitBreaker.getEventPublisher().onError(event -> failureMeter.mark());
    circuitBreaker.getEventPublisher().onCallNotPermitted(event -> unpermittedMeter.mark());
  }

}
