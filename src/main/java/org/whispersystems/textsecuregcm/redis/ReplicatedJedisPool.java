package org.whispersystems.textsecuregcm.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;

public class ReplicatedJedisPool {

  private final Logger        logger       = LoggerFactory.getLogger(ReplicatedJedisPool.class);
  private final AtomicInteger replicaIndex = new AtomicInteger(0);

  private final JedisPool   master;
  private final JedisPool[] replicas;

  public ReplicatedJedisPool(JedisPool master, List<JedisPool> replicas) {
    if (replicas.size() < 1) throw new IllegalArgumentException("There must be at least one replica");

    this.master   = master;
    this.replicas = new JedisPool[replicas.size()];

    for (int i=0;i<this.replicas.length;i++) {
      this.replicas[i] = replicas.get(i);
    }
  }

  public Jedis getWriteResource() {
    return master.getResource();
  }

  public void returnWriteResource(Jedis jedis) {
    master.returnResource(jedis);
  }

  public Jedis getReadResource() {
    int failureCount = 0;

    while (failureCount < replicas.length) {
      try {
        return replicas[replicaIndex.getAndIncrement() % replicas.length].getResource();
      } catch (JedisException e) {
        logger.error("Failure obtaining read replica pool", e);
      }

      failureCount++;
    }

    throw new JedisException("All read replica pools failed!");
  }

}
