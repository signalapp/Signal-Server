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
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import reactor.core.publisher.Flux;

class RegenerateAccountConstraintDataCommandTest {

  private Accounts accounts;

  private static class TestRegenerateAccountConstraintDataCommand extends RegenerateAccountConstraintDataCommand {

    private final CommandDependencies commandDependencies;
    private final Namespace namespace;

    TestRegenerateAccountConstraintDataCommand(final Accounts accounts, final boolean dryRun) {
      commandDependencies = new CommandDependencies(accounts,
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
          null,
          null);

      namespace = new Namespace(Map.of(
          RegenerateAccountConstraintDataCommand.DRY_RUN_ARGUMENT, dryRun,
          RegenerateAccountConstraintDataCommand.MAX_CONCURRENCY_ARGUMENT, 16,
          RegenerateAccountConstraintDataCommand.RETRIES_ARGUMENT, 3));
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
    accounts = mock(Accounts.class);

    when(accounts.regenerateConstraints(any())).thenReturn(CompletableFuture.completedFuture(null));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void crawlAccounts(final boolean dryRun) {
    final Account account = mock(Account.class);

    final RegenerateAccountConstraintDataCommand regenerateAccountConstraintDataCommand =
        new TestRegenerateAccountConstraintDataCommand(accounts, dryRun);

    regenerateAccountConstraintDataCommand.crawlAccounts(Flux.just(account));

    if (!dryRun) {
      verify(accounts).regenerateConstraints(account);
    }

    verifyNoMoreInteractions(accounts);
  }
}
