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


import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.hystrix.HystrixCommand;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.exception.HystrixBadRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.hystrix.GroupKeys;
import org.whispersystems.textsecuregcm.redis.ReplicatedJedisPool;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import redis.clients.jedis.Jedis;

public class AccountsManager {

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

  public long getCount() {
    return accounts.getCount();
  }

  public List<Account> getAll(int offset, int length) {
    return accounts.getAll(offset, length);
  }

  public Iterator<Account> getAll() {
    return accounts.getAll();
  }

  public boolean create(Account account) {
    boolean freshUser = databaseCreate(account);
    redisSet(account.getNumber(), account, false);
    updateDirectory(account);

    return freshUser;
  }

  public void update(Account account) {
    redisSet(account.getNumber(), account, false);
    databaseUpdate(account);
    updateDirectory(account);
  }

  public Optional<Account> get(String number) {
    Optional<Account> account = redisGet(number);

    if (!account.isPresent()) {
      account = databaseGet(number);
      account.ifPresent(value -> redisSet(number, value, true));
    }

    return account;
  }

  private void updateDirectory(Account account) {
    new HystrixCommand<Void>(HystrixCommandGroupKey.Factory.asKey(GroupKeys.DIRECTORY_SERVICE)) {
      @Override
      protected Void run() {
        if (account.isActive()) {
          byte[]        token         = Util.getContactToken(account.getNumber());
          ClientContact clientContact = new ClientContact(token, null, true, true);
          directory.add(clientContact);
        } else {
          directory.remove(account.getNumber());
        }

        return null;
      }
    }.execute();
  }

  private String getKey(String number) {
    return Account.class.getSimpleName() + Account.MEMCACHE_VERION + number;
  }

  private void redisSet(String number, Account account, boolean optional) {
    new HystrixCommand<Boolean>(HystrixCommandGroupKey.Factory.asKey(GroupKeys.REDIS_CACHE)) {
      @Override
      protected Boolean run() {
        try (Jedis jedis = cacheClient.getWriteResource()) {
          jedis.set(getKey(number), mapper.writeValueAsString(account));
        } catch (JsonProcessingException e) {
          throw new HystrixBadRequestException("Json processing error", e);
        }

        return true;
      }

      @Override
      protected Boolean getFallback() {
        if (optional) return true;
        else          return super.getFallback();
      }
    }.execute();
  }

  private Optional<Account> redisGet(String number) {
    return new HystrixCommand<Optional<Account>>(HystrixCommandGroupKey.Factory.asKey(GroupKeys.REDIS_CACHE)) {
      @Override
      protected Optional<Account> run() {
        try (Jedis jedis = cacheClient.getReadResource()) {
          String json = jedis.get(getKey(number));

          if (json != null) return Optional.of(mapper.readValue(json, Account.class));
          else              return Optional.empty();
        } catch (IOException e) {
          logger.warn("AccountsManager", "Deserialization error", e);
          return Optional.empty();
        }
      }

      @Override
      protected Optional<Account> getFallback() {
        return Optional.empty();
      }
    }.execute();
  }

  private Optional<Account> databaseGet(String number) {
    return new HystrixCommand<Optional<Account>>(HystrixCommandGroupKey.Factory.asKey(GroupKeys.DATABASE_ACCOUNTS)) {
      @Override
      protected Optional<Account> run() {
        return Optional.ofNullable(accounts.get(number));
      }
    }.execute();
  }

  private boolean databaseCreate(Account account) {
    return new HystrixCommand<Boolean>(HystrixCommandGroupKey.Factory.asKey(GroupKeys.DATABASE_ACCOUNTS)) {
      @Override
      protected Boolean run() {
        return accounts.create(account);
      }
    }.execute();
  }

  private void databaseUpdate(Account account) {
    new HystrixCommand<Void>(HystrixCommandGroupKey.Factory.asKey(GroupKeys.DATABASE_ACCOUNTS)) {
      @Override
      protected Void run() {
        accounts.update(account);
        return null;
      }
    }.execute();
  }
}
