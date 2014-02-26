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
import net.spy.memcached.MemcachedClient;
import org.whispersystems.textsecuregcm.entities.ClientContact;
import org.whispersystems.textsecuregcm.util.Util;

import java.util.Iterator;
import java.util.List;

public class AccountsManager {

  private final Accounts         accounts;
  private final MemcachedClient  memcachedClient;
  private final DirectoryManager directory;

  public AccountsManager(Accounts accounts,
                         DirectoryManager directory,
                         MemcachedClient memcachedClient)
  {
    this.accounts        = accounts;
    this.directory       = directory;
    this.memcachedClient = memcachedClient;
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

    if (memcachedClient != null) {
      memcachedClient.set(getKey(account.getNumber()), 0, account);
    }

    updateDirectory(account);
  }

  public void update(Account account) {
    if (memcachedClient != null) {
      memcachedClient.set(getKey(account.getNumber()), 0, account);
    }

    accounts.update(account);
    updateDirectory(account);
  }

  public Optional<Account> get(String number) {
    Account account = null;

    if (memcachedClient != null) {
      account = (Account)memcachedClient.get(getKey(number));
    }

    if (account == null) {
      account = accounts.get(number);

      if (account != null && memcachedClient != null) {
        memcachedClient.set(getKey(number), 0, account);
      }
    }

    if (account != null) return Optional.of(account);
    else                 return Optional.absent();
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

}
