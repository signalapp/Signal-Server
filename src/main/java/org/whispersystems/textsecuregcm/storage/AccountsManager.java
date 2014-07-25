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


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import net.spy.memcached.MemcachedClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.Util;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class AccountsManager {

  private final Logger logger = LoggerFactory.getLogger(AccountsManager.class);

  private final Accounts         accounts;
  private final MemcachedClient  memcachedClient;
  private final DirectoryManager directory;
  private final ObjectMapper     mapper;

  public AccountsManager(Accounts accounts,
                         DirectoryManager directory,
                         MemcachedClient memcachedClient)
  {
    this.accounts        = accounts;
    this.directory       = directory;
    this.memcachedClient = memcachedClient;
    this.mapper          = SystemMapper.getMapper();
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

  public void create(Account account) {
    accounts.create(account);
    memcacheSet(account.getNumber(), account);
    updateDirectory(account);
  }

  public void update(Account account) {
    memcacheSet(account.getNumber(), account);
    accounts.update(account);
    updateDirectory(account);
  }

  public Optional<Account> get(String number) {
    Optional<Account> account = memcacheGet(number);

    if (!account.isPresent()) {
      account = Optional.fromNullable(accounts.get(number));

      if (account.isPresent()) {
        memcacheSet(number, account.get());
      }
    }

    return account;
  }

  public boolean isRelayListed(String number) {
    byte[]                  token   = Util.getContactToken(number);
    Optional<ClientContact> contact = directory.get(token);

    return contact.isPresent() && !Util.isEmpty(contact.get().getRelay());
  }

  private void updateDirectory(Account account) {
    if (account.isActive()) {
      byte[]        token         = Util.getContactToken(account.getNumber());
      ClientContact clientContact = new ClientContact(token, null, account.getSupportsSms());
      directory.add(clientContact);
    } else {
      directory.remove(account.getNumber());
    }
  }

  private String getKey(String number) {
    return Account.class.getSimpleName() + Account.MEMCACHE_VERION + number;
  }

  private void memcacheSet(String number, Account account) {
    if (memcachedClient != null) {
      try {
        String json = mapper.writeValueAsString(account);
        memcachedClient.set(getKey(number), 0, json);
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  private Optional<Account> memcacheGet(String number) {
    if (memcachedClient == null) return Optional.absent();

    try {
      String json = (String)memcachedClient.get(getKey(number));

      if (json != null) return Optional.of(mapper.readValue(json, Account.class));
      else              return Optional.absent();

    } catch (IOException e) {
      logger.warn("AccountsManager", "Deserialization error", e);
      return Optional.absent();
    }
  }

}
