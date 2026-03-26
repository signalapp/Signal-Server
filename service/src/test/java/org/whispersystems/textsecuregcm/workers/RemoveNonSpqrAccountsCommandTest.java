/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import reactor.core.publisher.Flux;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

class RemoveNonSpqrAccountsCommandTest {

  private static class TestRemoveNonSpqrAccountsCommand extends RemoveNonSpqrAccountsCommand {

    private final CommandDependencies commandDependencies;
    private final Namespace namespace;

    public TestRemoveNonSpqrAccountsCommand(final int maxAccounts, final boolean isDryRun) {

      commandDependencies = mock(CommandDependencies.class);
      when(commandDependencies.accountsManager()).thenReturn(mock(AccountsManager.class));

      namespace = new Namespace(Map.of(
          RemoveNonSpqrAccountsCommand.MAX_ACCOUNTS_ARGUMENT, maxAccounts,
          RemoveNonSpqrAccountsCommand.DRY_RUN_ARGUMENT, isDryRun,
          RemoveNonSpqrAccountsCommand.MAX_CONCURRENCY_ARGUMENT, 16));
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
    final int maxAccountsToRemove = 4;

    final List<Account> accounts = new ArrayList<>();

    for (int i = 0; i < maxAccountsToRemove * 2; i++) {
      accounts.add(buildMockAccount(true));
    }

    for (int i = 0; i < maxAccountsToRemove * 2; i++) {
      accounts.add(buildMockAccount(false));
    }

    Collections.shuffle(accounts);

    final RemoveNonSpqrAccountsCommand removeNonSpqrAccountsCommand =
        new TestRemoveNonSpqrAccountsCommand(maxAccountsToRemove, dryRun);

    removeNonSpqrAccountsCommand.crawlAccounts(Flux.fromIterable(accounts));

    final AccountsManager accountsManager = removeNonSpqrAccountsCommand.getCommandDependencies().accountsManager();

    if (dryRun) {
      verify(accountsManager, never()).delete(any(), any());
    } else {
      verify(accountsManager, times(maxAccountsToRemove)).delete(
          argThat(account -> !account.getPrimaryDevice().hasCapability(DeviceCapability.SPARSE_POST_QUANTUM_RATCHET)),
          eq(AccountsManager.DeletionReason.ADMIN_DELETED));

      verifyNoMoreInteractions(accountsManager);
    }
  }

  private static Account buildMockAccount(final boolean supportsSpqr) {
    final Device primaryDevice = mock(Device.class);
    when(primaryDevice.hasCapability(DeviceCapability.SPARSE_POST_QUANTUM_RATCHET)).thenReturn(supportsSpqr);

    final Account account = mock(Account.class);
    when(account.getPrimaryDevice()).thenReturn(primaryDevice);

    return account;
  }
}
