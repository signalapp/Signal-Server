/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.entities.ECSignedPreKey;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import reactor.core.publisher.Flux;

class RemoveLinkedDevicesWithoutPniKeysCommandTest {

  private AccountsManager accountsManager;
  private KeysManager keysManager;

  private static class TestRemoveLinkedDevicesWithoutPniKeysCommand extends RemoveLinkedDevicesWithoutPniKeysCommand {

    private final CommandDependencies commandDependencies;
    private final Namespace namespace;

    TestRemoveLinkedDevicesWithoutPniKeysCommand(final AccountsManager accountsManager,
        final KeysManager keysManager,
        final boolean dryRun) {

      commandDependencies = new CommandDependencies(accountsManager,
          null,
          null,
          null,
          null,
          keysManager,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null);

      namespace = new Namespace(Map.of(
          RemoveLinkedDevicesWithoutPqKeysCommand.DRY_RUN_ARGUMENT, dryRun,
          RemoveLinkedDevicesWithoutPqKeysCommand.MAX_CONCURRENCY_ARGUMENT, 16,
          RemoveLinkedDevicesWithoutPqKeysCommand.RETRIES_ARGUMENT, 3));
    }

    @Override
    protected CommandDependencies getCommandDependencies() {
      return commandDependencies;
    }

    @Override
    protected Namespace getNamespace() {
      return namespace;
    }
  }

  @BeforeEach
  void setUp() {
    accountsManager = mock(AccountsManager.class);
    keysManager = mock(KeysManager.class);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void crawlAccounts(final boolean dryRun) {
    final Account accountWithoutLinkedDevices = mock(Account.class);
    when(accountWithoutLinkedDevices.getDevices()).thenReturn(List.of(mock(Device.class)));

    final Device primaryDevice = mock(Device.class);
    when(primaryDevice.isPrimary()).thenReturn(true);

    final byte deviceIdWithPniKey = Device.PRIMARY_ID + 1;

    final Device linkedDeviceWithPniKey = mock(Device.class);
    when(linkedDeviceWithPniKey.isPrimary()).thenReturn(false);
    when(linkedDeviceWithPniKey.getId()).thenReturn(deviceIdWithPniKey);

    final UUID pniWithLinkedDeviceWithPniKey = UUID.randomUUID();

    final Account accountWithLinkedDeviceWithPniKey = mock(Account.class);
    when(accountWithLinkedDeviceWithPniKey.getIdentifier(IdentityType.PNI)).thenReturn(pniWithLinkedDeviceWithPniKey);
    when(accountWithLinkedDeviceWithPniKey.getDevices()).thenReturn(List.of(primaryDevice, linkedDeviceWithPniKey));

    final byte deviceIdWithoutPniKey = deviceIdWithPniKey + 1;

    final Device linkedDeviceWithoutPniKey = mock(Device.class);
    when(linkedDeviceWithoutPniKey.isPrimary()).thenReturn(false);
    when(linkedDeviceWithoutPniKey.getId()).thenReturn(deviceIdWithoutPniKey);

    final Account accountWithLinkedDeviceWithoutPniKey = mock(Account.class);
    when(accountWithLinkedDeviceWithoutPniKey.getDevices()).thenReturn(List.of(primaryDevice, linkedDeviceWithoutPniKey));

    when(accountsManager.removeDevice(any(), anyByte())).thenAnswer(invocation -> {
      final Account account = invocation.getArgument(0);
      return CompletableFuture.completedFuture(account);
    });

    when(keysManager.getEcSignedPreKey(any(), anyByte()))
        .thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    when(keysManager.getEcSignedPreKey(pniWithLinkedDeviceWithPniKey, deviceIdWithPniKey))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(mock(ECSignedPreKey.class))));

    final RemoveLinkedDevicesWithoutPniKeysCommand removeLinkedDevicesWithoutPniKeysCommand =
        new TestRemoveLinkedDevicesWithoutPniKeysCommand(accountsManager, keysManager, dryRun);

    removeLinkedDevicesWithoutPniKeysCommand.crawlAccounts(Flux.just(
        accountWithoutLinkedDevices, accountWithLinkedDeviceWithPniKey, accountWithLinkedDeviceWithoutPniKey));

    if (!dryRun) {
      verify(accountsManager).removeDevice(accountWithLinkedDeviceWithoutPniKey, deviceIdWithoutPniKey);
    }

    verifyNoMoreInteractions(accountsManager);
  }
}
