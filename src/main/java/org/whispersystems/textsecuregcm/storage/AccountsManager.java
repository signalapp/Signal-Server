/*
 * Copyright (C) 2013-2018 Signal
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


import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;

import java.io.IOException;
import java.util.Optional;

import static com.codahale.metrics.MetricRegistry.name;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

public class AccountsManager {

  private static final MetricRegistry metricRegistry      = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          createTimer         = metricRegistry.timer(name(AccountsManager.class, "create"        ));
  private static final Timer          updateTimer         = metricRegistry.timer(name(AccountsManager.class, "update"        ));
  private static final Timer          getTimer            = metricRegistry.timer(name(AccountsManager.class, "get"           ));

  private static final Timer          redisSetTimer       = metricRegistry.timer(name(AccountsManager.class, "redisSet"      ));
  private static final Timer          redisGetTimer       = metricRegistry.timer(name(AccountsManager.class, "redisGet"      ));

  private final Logger logger = LoggerFactory.getLogger(AccountsManager.class);

  private final Accounts            accounts;
  private final ReplicatedJedisPool cacheClient;
  private final DirectoryManager    directory;
  private final ObjectMapper        mapper;

  public AccountsManager(Accounts accounts, DirectoryManager directory, ReplicatedJedisPool cacheClient) {
    this.accounts    = accounts;
    this.directory   = directory;
    this.cacheClient = cacheClient;
    this.mapper      = SystemMapper.getMapper();
  }

  public boolean create(Account account) {
    try (Timer.Context context = createTimer.time()) {
      boolean freshUser = databaseCreate(account);
      redisSet(account.getNumber(), account, false);
      updateDirectory(account);

      return freshUser;
    }
  }

  public void update(Account account) {
    try (Timer.Context context = updateTimer.time()) {
      redisSet(account.getNumber(), account, false);
      databaseUpdate(account);
      updateDirectory(account);
    }
  }

  public Optional<Account> get(String number) {
    try (Timer.Context context = getTimer.time()) {
      Optional<Account> account = redisGet(number);

      if (!account.isPresent()) {
        account = databaseGet(number);
        account.ifPresent(value -> redisSet(number, value, true));
      }

      return account;
    }
  }

  private void updateDirectory(Account account) {
    if (account.isActive()) {
      byte[]        token         = Util.getContactToken(account.getNumber());
      ClientContact clientContact = new ClientContact(token, null, true, true);
      directory.add(clientContact);
    } else {
      directory.remove(account.getNumber());
    }
  }

  private String getKey(String number) {
    return Account.class.getSimpleName() + Account.MEMCACHE_VERION + number;
  }

  private void redisSet(String number, Account account, boolean optional) {
    try (Jedis         jedis = cacheClient.getWriteResource();
         Timer.Context timer = redisSetTimer.time())
    {
      jedis.set(getKey(number), mapper.writeValueAsString(account));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  private Optional<Account> redisGet(String number) {
    try (Jedis         jedis = cacheClient.getReadResource();
         Timer.Context timer = redisGetTimer.time())
    {
      String json = jedis.get(getKey(number));

      if (json != null) {
        Account account = mapper.readValue(json, Account.class);
        account.setNumber(number);

        return Optional.of(account);
      }

      return Optional.empty();
    } catch (IOException e) {
      logger.warn("AccountsManager", "Deserialization error", e);
      return Optional.empty();
    } catch (JedisException e) {
      logger.warn("Redis failure", e);
      return Optional.empty();
    }
  }

  private Optional<Account> databaseGet(String number) {
    return accounts.get(number);
  }

  private boolean databaseCreate(Account account) {
    return accounts.create(account);
  }

  private void databaseUpdate(Account account) {
    accounts.update(account);
  }
}
