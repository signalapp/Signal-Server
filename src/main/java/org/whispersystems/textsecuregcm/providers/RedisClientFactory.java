/**
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.providers;

import org.whispersystems.textsecuregcm.configuration.RedisConfiguration;
import org.whispersystems.textsecuregcm.util.Util;

import java.net.URI;
import java.net.URISyntaxException;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public class RedisClientFactory {

  private final JedisPool jedisPool;

  public RedisClientFactory(RedisConfiguration redisConfig) throws URISyntaxException {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setTestOnBorrow(true);

    URI    redisURI      = new URI(redisConfig.getUrl());
    String redisHost     = redisURI.getHost();
    int    redisPort     = redisURI.getPort();
    String redisPassword = null;

    if (!Util.isEmpty(redisURI.getUserInfo())) {
      redisPassword = redisURI.getUserInfo().split(":",2)[1];
    }

    this.jedisPool = new JedisPool(poolConfig, redisHost, redisPort,
                                   Protocol.DEFAULT_TIMEOUT, redisPassword);
  }

  public JedisPool getRedisClientPool() {
    return jedisPool;
  }

}
