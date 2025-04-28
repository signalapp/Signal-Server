/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import reactor.core.publisher.Flux;

class RemoveAccountsWithoutPqKeysCommandTest {

  private AccountsManager accountsManager;
  private KeysManager keysManager;

  private static class TestRemoveAccountsWithoutPqKeysCommand extends RemoveAccountsWithoutPqKeysCommand {

    private final CommandDependencies commandDependencies;
    private final Namespace namespace;

    TestRemoveAccountsWithoutPqKeysCommand(final AccountsManager accountsManager,
        final KeysManager keysManager,
        final int maxAccounts,
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
          RemoveAccountsWithoutPqKeysCommand.DRY_RUN_ARGUMENT, dryRun,
          RemoveAccountsWithoutPqKeysCommand.MAX_CONCURRENCY_ARGUMENT, 16,
          RemoveAccountsWithoutPqKeysCommand.RETRIES_ARGUMENT, 3,
          RemoveAccountsWithoutPqKeysCommand.MAX_ACCOUNTS_ARGUMENT, maxAccounts));
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
    final UUID accountIdentifierWithPqKeys = UUID.randomUUID();

    final Account accountWithPqKeys = mock(Account.class);
    when(accountWithPqKeys.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifierWithPqKeys);
    when(accountWithPqKeys.getPrimaryDevice()).thenReturn(mock(Device.class));
    when(accountWithPqKeys.hasLockedCredentials()).thenReturn(true);

    when(keysManager.getLastResort(any(), anyByte())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));
    when(keysManager.getLastResort(accountIdentifierWithPqKeys, Device.PRIMARY_ID))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(mock(KEMSignedPreKey.class))));

    when(accountsManager.delete(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final int maxAccounts = 5;

    final RemoveAccountsWithoutPqKeysCommand removeAccountsWithoutPqKeysCommand =
        new TestRemoveAccountsWithoutPqKeysCommand(accountsManager, keysManager, maxAccounts, dryRun);

    removeAccountsWithoutPqKeysCommand.crawlAccounts(Flux.concat(
        Flux.just(accountWithPqKeys),
        Flux.generate(sink -> {
          final Account accountWithoutPqKeys = mock(Account.class);
          when(accountWithoutPqKeys.getIdentifier(IdentityType.ACI)).thenReturn(UUID.randomUUID());
          when(accountWithoutPqKeys.getPrimaryDevice()).thenReturn(mock(Device.class));
          when(accountWithoutPqKeys.hasLockedCredentials()).thenReturn(true);

          sink.next(accountWithoutPqKeys);
        })));

    if (dryRun) {
      verify(accountsManager, never()).delete(any(), any());
    } else {
      verify(accountsManager, times(maxAccounts)).delete(any(), eq(AccountsManager.DeletionReason.ADMIN_DELETED));
      verify(accountsManager, never()).delete(eq(accountWithPqKeys), any());
    }
  }
}
