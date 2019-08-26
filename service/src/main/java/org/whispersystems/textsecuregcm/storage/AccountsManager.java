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
import org.whispersystems.textsecuregcm.auth.AmbiguousIdentifier;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisException;

public class AccountsManager {

  private static final MetricRegistry metricRegistry   = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private static final Timer          createTimer      = metricRegistry.timer(name(AccountsManager.class, "create"     ));
  private static final Timer          updateTimer      = metricRegistry.timer(name(AccountsManager.class, "update"     ));
  private static final Timer          getByNumberTimer = metricRegistry.timer(name(AccountsManager.class, "getByNumber"));
  private static final Timer          getByUuidTimer   = metricRegistry.timer(name(AccountsManager.class, "getByUuid"  ));

  private static final Timer redisSetTimer       = metricRegistry.timer(name(AccountsManager.class, "redisSet"      ));
  private static final Timer redisNumberGetTimer = metricRegistry.timer(name(AccountsManager.class, "redisNumberGet"));
  private static final Timer redisUuidGetTimer   = metricRegistry.timer(name(AccountsManager.class, "redisUuidGet"  ));

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
    try (Timer.Context ignored = createTimer.time()) {
      boolean freshUser = databaseCreate(account);
      redisSet(account);
      updateDirectory(account);

      return freshUser;
    }
  }

  public void update(Account account) {
    try (Timer.Context ignored = updateTimer.time()) {
      redisSet(account);
      databaseUpdate(account);
      updateDirectory(account);
    }
  }

  public Optional<Account> get(AmbiguousIdentifier identifier) {
    if      (identifier.hasNumber()) return get(identifier.getNumber());
    else if (identifier.hasUuid())   return get(identifier.getUuid());
    else                             throw new AssertionError();
  }

  public Optional<Account> get(String number) {
    try (Timer.Context ignored = getByNumberTimer.time()) {
      Optional<Account> account = redisGet(number);

      if (!account.isPresent()) {
        account = databaseGet(number);
        account.ifPresent(value -> redisSet(value));
      }

      return account;
    }
  }

  public Optional<Account> get(UUID uuid) {
    try (Timer.Context ignored = getByUuidTimer.time()) {
      Optional<Account> account = redisGet(uuid);

      if (!account.isPresent()) {
        account = databaseGet(uuid);
        account.ifPresent(value -> redisSet(value));
      }

      return account;
    }
  }


  public List<Account> getAllFrom(int length) {
    return accounts.getAllFrom(length);
  }

  public List<Account> getAllFrom(UUID uuid, int length) {
    return accounts.getAllFrom(uuid, length);
  }

  private void updateDirectory(Account account) {
    if (account.isEnabled()) {
      byte[]        token         = Util.getContactToken(account.getNumber());
      ClientContact clientContact = new ClientContact(token, null, true, true);
      directory.add(clientContact);
    } else {
      directory.remove(account.getNumber());
    }
  }

  private String getAccountMapKey(String number) {
    return "AccountMap::" + number;
  }

  private String getAccountEntityKey(UUID uuid) {
    return "Account3::" + uuid.toString();
  }

  private void redisSet(Account account) {
    try (Jedis         jedis   = cacheClient.getWriteResource();
         Timer.Context ignored = redisSetTimer.time())
    {
      jedis.set(getAccountMapKey(account.getNumber()), account.getUuid().toString());
      jedis.set(getAccountEntityKey(account.getUuid()), mapper.writeValueAsString(account));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
  }

  private Optional<Account> redisGet(String number) {
    try (Jedis         jedis   = cacheClient.getReadResource();
         Timer.Context ignored = redisNumberGetTimer.time())
    {
      String uuid = jedis.get(getAccountMapKey(number));

      if (uuid != null) return redisGet(jedis, UUID.fromString(uuid));
      else              return Optional.empty();
    } catch (IllegalArgumentException e) {
      logger.warn("Deserialization error", e);
      return Optional.empty();
    } catch (JedisException e) {
      logger.warn("Redis failure", e);
      return Optional.empty();
    }
  }

  private Optional<Account> redisGet(UUID uuid) {
    try (Jedis jedis = cacheClient.getReadResource()) {
      return redisGet(jedis, uuid);
    }
  }

  private Optional<Account> redisGet(Jedis jedis, UUID uuid) {
    try (Timer.Context ignored = redisUuidGetTimer.time()) {
      String json = jedis.get(getAccountEntityKey(uuid));

      if (json != null) {
        Account account = mapper.readValue(json, Account.class);
        account.setUuid(uuid);

        return Optional.of(account);
      }

      return Optional.empty();
    } catch (IOException e) {
      logger.warn("Deserialization error", e);
      return Optional.empty();
    } catch (JedisException e) {
      logger.warn("Redis failure", e);
      return Optional.empty();
    }
  }

  private Optional<Account> databaseGet(String number) {
    return accounts.get(number);
  }

  private Optional<Account> databaseGet(UUID uuid) {
    return accounts.get(uuid);
  }

  private boolean databaseCreate(Account account) {
    return accounts.create(account);
  }

  private void databaseUpdate(Account account) {
    accounts.update(account);
  }
}
