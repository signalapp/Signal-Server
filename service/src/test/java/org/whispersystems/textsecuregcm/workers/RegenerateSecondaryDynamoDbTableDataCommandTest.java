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
import org.whispersystems.textsecuregcm.storage.DynamoDbRecoveryManager;
import reactor.core.publisher.Flux;

class RegenerateSecondaryDynamoDbTableDataCommandTest {

  private DynamoDbRecoveryManager dynamoDbRecoveryManager;

  private static class TestRegenerateSecondaryDynamoDbTableDataCommand extends RegenerateSecondaryDynamoDbTableDataCommand {

    private final CommandDependencies commandDependencies;
    private final Namespace namespace;

    TestRegenerateSecondaryDynamoDbTableDataCommand(final DynamoDbRecoveryManager dynamoDbRecoveryManager, final boolean dryRun) {
      commandDependencies = new CommandDependencies(null,
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
          dynamoDbRecoveryManager);

      namespace = new Namespace(Map.of(
          RegenerateSecondaryDynamoDbTableDataCommand.DRY_RUN_ARGUMENT, dryRun,
          RegenerateSecondaryDynamoDbTableDataCommand.MAX_CONCURRENCY_ARGUMENT, 16,
          RegenerateSecondaryDynamoDbTableDataCommand.RETRIES_ARGUMENT, 3));
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
    dynamoDbRecoveryManager = mock(DynamoDbRecoveryManager.class);

    when(dynamoDbRecoveryManager.regenerateData(any())).thenReturn(CompletableFuture.completedFuture(null));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void crawlAccounts(final boolean dryRun) {
    final Account account = mock(Account.class);

    final RegenerateSecondaryDynamoDbTableDataCommand regenerateSecondaryDynamoDbTableDataCommand =
        new TestRegenerateSecondaryDynamoDbTableDataCommand(dynamoDbRecoveryManager, dryRun);

    regenerateSecondaryDynamoDbTableDataCommand.crawlAccounts(Flux.just(account));

    if (!dryRun) {
      verify(dynamoDbRecoveryManager).regenerateData(account);
    }

    verifyNoMoreInteractions(dynamoDbRecoveryManager);
  }
}
