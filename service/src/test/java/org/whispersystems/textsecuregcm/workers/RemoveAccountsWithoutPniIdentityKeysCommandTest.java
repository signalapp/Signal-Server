/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.signal.libsignal.protocol.IdentityKey;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import reactor.core.publisher.Flux;

class RemoveAccountsWithoutPniIdentityKeysCommandTest {

  private AccountsManager accountsManager;

  private static class TestRemoveAccountsWithoutPniIdentityKeysCommand extends RemoveAccountsWithoutPniIdentityKeysCommand {

    private final CommandDependencies commandDependencies;
    private final Namespace namespace;

    TestRemoveAccountsWithoutPniIdentityKeysCommand(final AccountsManager accountsManager,
        final boolean dryRun) {

      commandDependencies = new CommandDependencies(accountsManager,
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
          null,
          null,
          null,
          null,
          null,
          null,
          null);

      namespace = new Namespace(Map.of(
          LockAccountsWithoutPqKeysCommand.DRY_RUN_ARGUMENT, dryRun,
          LockAccountsWithoutPqKeysCommand.MAX_CONCURRENCY_ARGUMENT, 16,
          LockAccountsWithoutPqKeysCommand.RETRIES_ARGUMENT, 3));
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
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void crawlAccounts(final boolean dryRun) {
    final Account accountWithPniIdentityKey = mock(Account.class);
    when(accountWithPniIdentityKey.getIdentityKey(IdentityType.PNI)).thenReturn(mock(IdentityKey.class));
    when(accountWithPniIdentityKey.getPrimaryDevice()).thenReturn(mock(Device.class));

    final Account lockedAccountWithoutPniIdentityKey = mock(Account.class);
    when(lockedAccountWithoutPniIdentityKey.hasLockedCredentials()).thenReturn(true);
    when(lockedAccountWithoutPniIdentityKey.getIdentityKey(IdentityType.PNI)).thenReturn(null);
    when(lockedAccountWithoutPniIdentityKey.getPrimaryDevice()).thenReturn(mock(Device.class));

    final Account unlockedAccountWithoutPniIdentityKey = mock(Account.class);
    when(unlockedAccountWithoutPniIdentityKey.hasLockedCredentials()).thenReturn(false);
    when(unlockedAccountWithoutPniIdentityKey.getIdentityKey(IdentityType.PNI)).thenReturn(null);
    when(unlockedAccountWithoutPniIdentityKey.getPrimaryDevice()).thenReturn(mock(Device.class));

    when(accountsManager.delete(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final RemoveAccountsWithoutPniIdentityKeysCommand removeAccountsWithoutPniIdentityKeysCommand =
        new TestRemoveAccountsWithoutPniIdentityKeysCommand(accountsManager, dryRun);

    removeAccountsWithoutPniIdentityKeysCommand.crawlAccounts(Flux.just(
        accountWithPniIdentityKey, lockedAccountWithoutPniIdentityKey, unlockedAccountWithoutPniIdentityKey));

    if (!dryRun) {
      verify(accountsManager).delete(lockedAccountWithoutPniIdentityKey, AccountsManager.DeletionReason.ADMIN_DELETED);
    }

    verifyNoMoreInteractions(accountsManager);
  }
}
