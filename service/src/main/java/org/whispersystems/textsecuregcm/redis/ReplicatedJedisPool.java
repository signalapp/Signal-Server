package org.whispersystems.textsecuregcm.redis;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import org.whispersystems.textsecuregcm.util.Constants;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

public class ReplicatedJedisPool {

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

    MetricRegistry       metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
    CircuitBreakerConfig config         = circuitBreakerConfiguration.toCircuitBreakerConfig();
    CircuitBreaker       masterBreaker  = CircuitBreaker.of(String.format("%s-master", name), config);

    CircuitBreakerUtil.registerMetrics(metricRegistry, masterBreaker, ReplicatedJedisPool.class);

    this.master   = CircuitBreaker.decorateSupplier(masterBreaker, master::getResource);
    this.replicas = new ArrayList<>(replicas.size());

    for (int i=0;i<replicas.size();i++) {
      JedisPool      replica      = replicas.get(i);
      CircuitBreaker slaveBreaker = CircuitBreaker.of(String.format("%s-slave-%d", name, i), config);

      CircuitBreakerUtil.registerMetrics(metricRegistry, slaveBreaker, ReplicatedJedisPool.class);
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

}
