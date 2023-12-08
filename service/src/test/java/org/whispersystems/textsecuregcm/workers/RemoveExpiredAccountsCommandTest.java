/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import reactor.core.publisher.Flux;

class RemoveExpiredAccountsCommandTest {

  private static class TestRemoveExpiredAccountsCommand extends RemoveExpiredAccountsCommand {

    private final CommandDependencies commandDependencies;
    private final Namespace namespace;

    public TestRemoveExpiredAccountsCommand(final Clock clock, final AccountsManager accountsManager, final boolean isDryRun) {
      super(clock);

      commandDependencies = mock(CommandDependencies.class);
      when(commandDependencies.accountsManager()).thenReturn(accountsManager);

      namespace = mock(Namespace.class);
      when(namespace.getBoolean(RemoveExpiredAccountsCommand.DRY_RUN_ARGUMENT)).thenReturn(isDryRun);
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
  void crawlAccounts(final boolean isDryRun) {
    final Clock clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

    final AccountsManager accountsManager = mock(AccountsManager.class);
    when(accountsManager.delete(any(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final RemoveExpiredAccountsCommand removeExpiredAccountsCommand =
        new TestRemoveExpiredAccountsCommand(clock, accountsManager, isDryRun);

    final Account activeAccount = mock(Account.class);
    when(activeAccount.getLastSeen()).thenReturn(clock.instant().toEpochMilli());

    final Account expiredAccount = mock(Account.class);
    when(expiredAccount.getLastSeen())
        .thenReturn(clock.instant().minus(RemoveExpiredAccountsCommand.MAX_IDLE_DURATION).minusMillis(1).toEpochMilli());

    removeExpiredAccountsCommand.crawlAccounts(Flux.just(activeAccount, expiredAccount));

    if (isDryRun) {
      verify(accountsManager, never()).delete(any(), any());
    } else {
      verify(accountsManager).delete(expiredAccount, AccountsManager.DeletionReason.EXPIRED);
      verify(accountsManager, never()).delete(eq(activeAccount), any());
    }
  }

  @ParameterizedTest
  @MethodSource
  void isExpired(final Instant currentTime, final Instant lastSeen, final boolean expectExpired) {
    final Clock clock = Clock.fixed(currentTime, ZoneId.systemDefault());

    final Account account = mock(Account.class);
    when(account.getLastSeen()).thenReturn(lastSeen.toEpochMilli());

    assertEquals(expectExpired, new RemoveExpiredAccountsCommand(clock).isExpired(account));
  }

  private static Stream<Arguments> isExpired() {
    final Instant currentTime = Instant.now();

    return Stream.of(
        Arguments.of(currentTime, currentTime, false),
        Arguments.of(currentTime, currentTime.minus(RemoveExpiredAccountsCommand.MAX_IDLE_DURATION).plusMillis(1), false),
        Arguments.of(currentTime, currentTime.minus(RemoveExpiredAccountsCommand.MAX_IDLE_DURATION), true),
        Arguments.of(currentTime, currentTime.minus(RemoveExpiredAccountsCommand.MAX_IDLE_DURATION).minusMillis(1), true)
    );
  }
}
