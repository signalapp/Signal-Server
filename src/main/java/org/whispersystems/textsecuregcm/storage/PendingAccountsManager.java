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
package org.whispersystems.textsecuregcm.storage;

import com.google.common.base.Optional;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class PendingAccountsManager {

  private static final String CACHE_PREFIX = "pending_account::";

  private final PendingAccounts pendingAccounts;
  private final JedisPool       cacheClient;

  public PendingAccountsManager(PendingAccounts pendingAccounts, JedisPool cacheClient)
  {
    this.pendingAccounts = pendingAccounts;
    this.cacheClient     = cacheClient;
  }

  public void store(String number, String code) {
    memcacheSet(number, code);
    pendingAccounts.insert(number, code);
  }

  public void remove(String number) {
    memcacheDelete(number);
    pendingAccounts.remove(number);
  }

  public Optional<String> getCodeForNumber(String number) {
    Optional<String> code = memcacheGet(number);

    if (!code.isPresent()) {
      code = Optional.fromNullable(pendingAccounts.getCodeForNumber(number));

      if (code.isPresent()) {
        memcacheSet(number, code.get());
      }
    }

    return code;
  }

  private void memcacheSet(String number, String code) {
    try (Jedis jedis = cacheClient.getResource()) {
      jedis.set(CACHE_PREFIX + number, code);
    }
  }

  private Optional<String> memcacheGet(String number) {
    try (Jedis jedis = cacheClient.getResource()) {
      return Optional.fromNullable(jedis.get(CACHE_PREFIX + number));
    }
  }

  private void memcacheDelete(String number) {
    try (Jedis jedis = cacheClient.getResource()) {
      jedis.del(CACHE_PREFIX + number);
    }
  }
}
