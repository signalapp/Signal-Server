/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import reactor.core.publisher.Flux;

class RemoveLinkedDevicesWithoutPqKeysCommandTest {

  private AccountsManager accountsManager;
  private KeysManager keysManager;

  private static class TestRemoveLinkedDevicesWithoutPqKeysCommand extends RemoveLinkedDevicesWithoutPqKeysCommand {

    private final CommandDependencies commandDependencies;
    private final Namespace namespace;

    TestRemoveLinkedDevicesWithoutPqKeysCommand(final AccountsManager accountsManager,
        final KeysManager keysManager,
        final boolean dryRun) {

      commandDependencies = new CommandDependencies(accountsManager,
          null,
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
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceIdWithPqKeys = Device.PRIMARY_ID + 1;
    final byte deviceIdWithoutPqKeys = deviceIdWithPqKeys + 1;

    final Device primaryDevice = mock(Device.class);
    when(primaryDevice.isPrimary()).thenReturn(true);
    when(primaryDevice.getId()).thenReturn(Device.PRIMARY_ID);

    final Device linkedDeviceWithPqKeys = mock(Device.class);
    when(linkedDeviceWithPqKeys.isPrimary()).thenReturn(false);
    when(linkedDeviceWithPqKeys.getId()).thenReturn(deviceIdWithPqKeys);

    final Device linkedDeviceWithoutPqKeys = mock(Device.class);
    when(linkedDeviceWithoutPqKeys.isPrimary()).thenReturn(false);
    when(linkedDeviceWithoutPqKeys.getId()).thenReturn(deviceIdWithoutPqKeys);

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(account.getDevices()).thenReturn(List.of(primaryDevice, linkedDeviceWithPqKeys, linkedDeviceWithoutPqKeys));

    when(keysManager.getPqEnabledDevices(accountIdentifier))
        .thenReturn(CompletableFuture.completedFuture(List.of(deviceIdWithPqKeys)));

    when(accountsManager.removeDevice(any(), anyByte()))
        .thenAnswer(invocation -> CompletableFuture.completedFuture(invocation.getArgument(0)));

    final RemoveLinkedDevicesWithoutPqKeysCommand removeLinkedDevicesWithoutPqKeysCommand =
        new TestRemoveLinkedDevicesWithoutPqKeysCommand(accountsManager, keysManager, dryRun);

    removeLinkedDevicesWithoutPqKeysCommand.crawlAccounts(Flux.just(account));

    if (dryRun) {
      verify(accountsManager, never()).removeDevice(any(), anyByte());
    } else {
      verify(accountsManager).removeDevice(account, deviceIdWithoutPqKeys);
      verifyNoMoreInteractions(accountsManager);
    }
  }
}
