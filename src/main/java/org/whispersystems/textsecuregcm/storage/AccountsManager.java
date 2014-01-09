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
import org.whispersystems.textsecuregcm.util.NumberData;
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
    return accounts.getNumberCount();
  }

  public List<NumberData> getAllNumbers(int offset, int length) {
    return accounts.getAllNumbers(offset, length);
  }

  public Iterator<NumberData> getAllNumbers() {
    return accounts.getAllNumbers();
  }

  /** Creates a new Account and NumberData, clearing all existing accounts/data on the given number */
  public void createResetNumber(Account account) {
    long id = accounts.insertClearingNumber(account);
    account.setId(id);

    if (memcachedClient != null) {
      memcachedClient.set(getKey(account.getNumber(), account.getDeviceId()), 0, account);
    }

    updateDirectory(account, false);
  }

  /** Creates a new Account for an existing NumberData (setting the deviceId) */
  public void createAccountOnExistingNumber(Account account) {
    long id = accounts.insert(account);
    account.setId(id);

    if (memcachedClient != null) {
      memcachedClient.set(getKey(account.getNumber(), account.getDeviceId()), 0, account);
    }

    updateDirectory(account, true);
  }

  public void update(Account account) {
    if (memcachedClient != null) {
      memcachedClient.set(getKey(account.getNumber(), account.getDeviceId()), 0, account);
    }

    accounts.update(account);
    updateDirectory(account, true);
  }

  public Optional<Account> get(String number, long deviceId) {
    Account account = null;

    if (memcachedClient != null) {
      account = (Account)memcachedClient.get(getKey(number, deviceId));
    }

    if (account == null) {
      account = accounts.get(number, deviceId);

      if (account != null && memcachedClient != null) {
        memcachedClient.set(getKey(number, deviceId), 0, account);
      }
    }

    if (account != null) return Optional.of(account);
    else                 return Optional.absent();
  }

  public List<Account> getAllByNumber(String number) {
    return accounts.getAllByNumber(number);
  }

  private void updateDirectory(Account account, boolean possiblyOtherAccounts) {
    boolean active = account.getFetchesMessages() ||
                     !Util.isEmpty(account.getApnRegistrationId()) || !Util.isEmpty(account.getGcmRegistrationId());
    boolean supportsSms = account.getSupportsSms();

    if (possiblyOtherAccounts && (!active || !supportsSms)) {
      NumberData numberData = accounts.getNumberData(account.getNumber());
      active = numberData.isActive();
      supportsSms = numberData.isSupportsSms();
    }

    if (active) {
      byte[]        token         = Util.getContactToken(account.getNumber());
      ClientContact clientContact = new ClientContact(token, null, supportsSms);
      directory.add(clientContact);
    } else {
      directory.remove(account.getNumber());
    }
  }

  private String getKey(String number, long accountId) {
    return Account.class.getSimpleName() + Account.MEMCACHE_VERION + number + accountId;
  }
}
