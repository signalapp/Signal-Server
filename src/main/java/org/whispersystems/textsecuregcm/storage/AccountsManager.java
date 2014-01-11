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
    return accounts.getNumberCount();
  }

  public List<Device> getAllMasterAccounts(int offset, int length) {
    return accounts.getAllFirstAccounts(offset, length);
  }

  public Iterator<Device> getAllMasterAccounts() {
    return accounts.getAllFirstAccounts();
  }

  /** Creates a new Device and NumberData, clearing all existing accounts/data on the given number */
  public void createResetNumber(Device device) {
    long id = accounts.insertClearingNumber(device);
    device.setId(id);

    if (memcachedClient != null) {
      memcachedClient.set(getKey(device.getNumber(), device.getDeviceId()), 0, device);
    }

    updateDirectory(device);
  }

  /** Creates a new Device for an existing NumberData (setting the deviceId) */
  public void createAccountOnExistingNumber(Device device) {
    long id = accounts.insert(device);
    device.setId(id);

    if (memcachedClient != null) {
      memcachedClient.set(getKey(device.getNumber(), device.getDeviceId()), 0, device);
    }

    updateDirectory(device);
  }

  public void update(Device device) {
    if (memcachedClient != null) {
      memcachedClient.set(getKey(device.getNumber(), device.getDeviceId()), 0, device);
    }

    accounts.update(device);
    updateDirectory(device);
  }

  public Optional<Device> get(String number, long deviceId) {
    Device device = null;

    if (memcachedClient != null) {
      device = (Device)memcachedClient.get(getKey(number, deviceId));
    }

    if (device == null) {
      device = accounts.get(number, deviceId);

      if (device != null && memcachedClient != null) {
        memcachedClient.set(getKey(number, deviceId), 0, device);
      }
    }

    if (device != null) return Optional.of(device);
    else                 return Optional.absent();
  }

  public List<Device> getAllByNumber(String number) {
    return accounts.getAllByNumber(number);
  }

  private void updateDirectory(Device device) {
    if (device.getDeviceId() != 1)
      return;

    if (device.isActive()) {
      byte[]        token         = Util.getContactToken(device.getNumber());
      ClientContact clientContact = new ClientContact(token, null, device.getSupportsSms());
      directory.add(clientContact);
    } else {
      directory.remove(device.getNumber());
    }
  }

  private String getKey(String number, long accountId) {
    return Device.class.getSimpleName() + Device.MEMCACHE_VERION + number + accountId;
  }
}
