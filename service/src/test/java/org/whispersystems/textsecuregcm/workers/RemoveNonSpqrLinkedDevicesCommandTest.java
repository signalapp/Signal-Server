/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import reactor.core.publisher.Flux;

class RemoveNonSpqrLinkedDevicesCommandTest {

  private static class TestRemoveNonSpqrLinkedDevicesCommand extends RemoveNonSpqrLinkedDevicesCommand {

    private final CommandDependencies commandDependencies;
    private final Namespace namespace;

    public TestRemoveNonSpqrLinkedDevicesCommand(final boolean isDryRun) {

      commandDependencies = mock(CommandDependencies.class);
      when(commandDependencies.accountsManager()).thenReturn(mock(AccountsManager.class));

      namespace = new Namespace(Map.of(
          RemoveNonSpqrLinkedDevicesCommand.DRY_RUN_ARGUMENT, isDryRun,
          RemoveNonSpqrLinkedDevicesCommand.MAX_CONCURRENCY_ARGUMENT, 16));
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

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void crawlAccounts(final boolean dryRun) {
    final Device primaryDeviceWithSpqr = buildMockDevice(true, true);
    final Device primaryDeviceWithoutSpqr = buildMockDevice(true, false);
    final Device linkedDeviceWithSpqr = buildMockDevice(false, true);
    final Device linkedDeviceWithoutSpqr = buildMockDevice(false, false);

    final Account accountWithNonSpqrPrimary = mock(Account.class);
    when(accountWithNonSpqrPrimary.getDevices())
        .thenReturn(List.of(primaryDeviceWithoutSpqr));

    final Account accountWithSpqrLinkedDevice = mock(Account.class);
    when(accountWithSpqrLinkedDevice.getDevices())
        .thenReturn(List.of(primaryDeviceWithSpqr, linkedDeviceWithSpqr));

    final Account accountWithNonSpqrLinkedDevice = mock(Account.class);
    when(accountWithNonSpqrLinkedDevice.getDevices())
        .thenReturn(List.of(primaryDeviceWithSpqr, linkedDeviceWithoutSpqr));

    final RemoveNonSpqrLinkedDevicesCommand removeNonSpqrLinkedDevicesCommand =
        new TestRemoveNonSpqrLinkedDevicesCommand(dryRun);

    removeNonSpqrLinkedDevicesCommand.crawlAccounts(Flux.just(
        accountWithNonSpqrPrimary, accountWithSpqrLinkedDevice, accountWithNonSpqrLinkedDevice));

    final AccountsManager accountsManager =
        removeNonSpqrLinkedDevicesCommand.getCommandDependencies().accountsManager();

    if (dryRun) {
      verifyNoInteractions(accountsManager);
    } else {
      verify(accountsManager).removeDevice(accountWithNonSpqrLinkedDevice, linkedDeviceWithoutSpqr.getId());
      verifyNoMoreInteractions(accountsManager);
    }
  }

  private Device buildMockDevice(final boolean isPrimary, final boolean supportsSpqr) {
    final Device device = mock(Device.class);
    when(device.isPrimary()).thenReturn(isPrimary);
    when(device.getId()).thenReturn(isPrimary ? Device.PRIMARY_ID : Device.PRIMARY_ID + 1);
    when(device.hasCapability(DeviceCapability.SPARSE_POST_QUANTUM_RATCHET)).thenReturn(supportsSpqr);

    return device;
  }
}
