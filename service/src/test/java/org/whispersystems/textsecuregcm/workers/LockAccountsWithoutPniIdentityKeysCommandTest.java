/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.signal.libsignal.protocol.IdentityKey;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import reactor.core.publisher.Flux;

class LockAccountsWithoutPniIdentityKeysCommandTest {

  private AccountsManager accountsManager;

  private static class TestLockAccountsWithoutPniIdentityKeysCommand extends LockAccountsWithoutPniIdentityKeysCommand {

    private final CommandDependencies commandDependencies;
    private final Namespace namespace;

    TestLockAccountsWithoutPniIdentityKeysCommand(final AccountsManager accountsManager,
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

    final Account accountWithoutPniIdentityKey = mock(Account.class);
    when(accountWithoutPniIdentityKey.getIdentityKey(IdentityType.PNI)).thenReturn(null);
    when(accountWithoutPniIdentityKey.getPrimaryDevice()).thenReturn(mock(Device.class));

    when(accountsManager.updateAsync(any(), any())).thenAnswer(invocation -> {
      final Account account = invocation.getArgument(0);
      final Consumer<Account> updater = invocation.getArgument(1);

      updater.accept(account);

      return CompletableFuture.completedFuture(account);
    });

    final LockAccountsWithoutPniIdentityKeysCommand lockAccountsWithoutPniIdentityKeysCommand =
        new TestLockAccountsWithoutPniIdentityKeysCommand(accountsManager, dryRun);

    lockAccountsWithoutPniIdentityKeysCommand.crawlAccounts(
        Flux.just(accountWithPniIdentityKey, accountWithoutPniIdentityKey));

    if (!dryRun) {
      verify(accountsManager).updateAsync(eq(accountWithoutPniIdentityKey), any());
      verify(accountWithoutPniIdentityKey).lockAuthTokenHash();
    }

    verifyNoMoreInteractions(accountsManager);
  }
}
