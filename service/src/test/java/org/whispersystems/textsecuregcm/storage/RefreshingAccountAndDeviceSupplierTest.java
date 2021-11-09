/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.Pair;

class RefreshingAccountAndDeviceSupplierTest {

  @Test
  void test() {

    final AccountsManager accountsManager = mock(AccountsManager.class);

    final UUID uuid = UUID.randomUUID();
    final long deviceId = 2L;

    final Account initialAccount = mock(Account.class);
    final Device initialDevice = mock(Device.class);

    when(initialAccount.getUuid()).thenReturn(uuid);
    when(initialDevice.getId()).thenReturn(deviceId);
    when(initialAccount.getDevice(deviceId)).thenReturn(Optional.of(initialDevice));

    when(accountsManager.getByAccountIdentifier(any(UUID.class))).thenAnswer(answer -> {
      final Account account = mock(Account.class);
      final Device device = mock(Device.class);

      when(account.getUuid()).thenReturn(answer.getArgument(0, UUID.class));
      when(account.getDevice(deviceId)).thenReturn(Optional.of(device));
      when(device.getId()).thenReturn(deviceId);

      return Optional.of(account);
    });

    final RefreshingAccountAndDeviceSupplier refreshingAccountAndDeviceSupplier = new RefreshingAccountAndDeviceSupplier(
        initialAccount, deviceId, accountsManager);

    Pair<Account, Device> accountAndDevice = refreshingAccountAndDeviceSupplier.get();

    assertSame(initialAccount, accountAndDevice.first());
    assertSame(initialDevice, accountAndDevice.second());

    accountAndDevice = refreshingAccountAndDeviceSupplier.get();

    assertSame(initialAccount, accountAndDevice.first());
    assertSame(initialDevice, accountAndDevice.second());

    when(initialAccount.isStale()).thenReturn(true);

    accountAndDevice = refreshingAccountAndDeviceSupplier.get();

    assertNotSame(initialAccount, accountAndDevice.first());
    assertNotSame(initialDevice, accountAndDevice.second());

    assertEquals(uuid, accountAndDevice.first().getUuid());
  }

}
