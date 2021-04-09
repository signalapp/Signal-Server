/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.providers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.dispatch.io.RedisPubSubConnectionFactory;
import org.whispersystems.dispatch.redis.PubSubConnection;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.util.Util;

import java.io.IOException;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.LinkedList;
import java.util.List;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public class RedisClientFactory implements RedisPubSubConnectionFactory {

  private final Logger logger = LoggerFactory.getLogger(RedisClientFactory.class);

  private final String    host;
  private final int       port;
  private final ReplicatedJedisPool jedisPool;

  public RedisClientFactory(String name, String url, List<String> replicaUrls, CircuitBreakerConfiguration circuitBreakerConfiguration)
      throws URISyntaxException
  {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setTestOnBorrow(true);
    poolConfig.setMaxWaitMillis(10000);

    URI redisURI = new URI(url);

    this.host      = redisURI.getHost();
    this.port      = redisURI.getPort();

    JedisPool       masterPool   = new JedisPool(poolConfig, host, port, Protocol.DEFAULT_TIMEOUT, null);
    List<JedisPool> replicaPools = new LinkedList<>();

    for (String replicaUrl : replicaUrls) {
      URI replicaURI = new URI(replicaUrl);

      replicaPools.add(new JedisPool(poolConfig, replicaURI.getHost(), replicaURI.getPort(),
                                     500, Protocol.DEFAULT_TIMEOUT, null,
                                     Protocol.DEFAULT_DATABASE, null, false, null ,
                                     null, null));
    }

    this.jedisPool = new ReplicatedJedisPool(name, masterPool, replicaPools, circuitBreakerConfiguration);
  }

  public ReplicatedJedisPool getRedisClientPool() {
    return jedisPool;
  }

  @Override
  public PubSubConnection connect() {
    while (true) {
      try {
        Socket socket = new Socket(host, port);
        return new PubSubConnection(socket);
      } catch (IOException e) {
        logger.warn("Error connecting", e);
        Util.sleep(200);
      }
    }
  }
}
