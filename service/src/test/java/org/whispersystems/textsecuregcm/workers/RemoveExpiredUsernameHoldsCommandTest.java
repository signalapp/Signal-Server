/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.stream.IntStream;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import reactor.core.publisher.Flux;

class RemoveExpiredUsernameHoldsCommandTest {

  private static class TestRemoveExpiredUsernameHoldsCommand extends RemoveExpiredUsernameHoldsCommand {

    private final CommandDependencies commandDependencies;
    private final Namespace namespace;

    public TestRemoveExpiredUsernameHoldsCommand(final Clock clock, final AccountsManager accountsManager,
        final boolean isDryRun) {
      super(clock);

      commandDependencies = mock(CommandDependencies.class);
      when(commandDependencies.accountsManager()).thenReturn(accountsManager);

      namespace = mock(Namespace.class);
      when(namespace.getBoolean(RemoveExpiredUsernameHoldsCommand.DRY_RUN_ARGUMENT)).thenReturn(isDryRun);
      when(namespace.getInt(RemoveExpiredUsernameHoldsCommand.MAX_CONCURRENCY_ARGUMENT)).thenReturn(16);
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
    final TestClock clock = TestClock.pinned(Instant.EPOCH.plus(Duration.ofSeconds(1)));

    final AccountsManager accountsManager = mock(AccountsManager.class);
    when(accountsManager.updateAsync(any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final RemoveExpiredUsernameHoldsCommand removeExpiredUsernameHoldsCommand =
        new TestRemoveExpiredUsernameHoldsCommand(clock, accountsManager, isDryRun);

    final Account hasHolds = mock(Account.class);
    final List<Account.UsernameHold> originalHolds = List.of(
        // expired
        new Account.UsernameHold(TestRandomUtil.nextBytes(32), Instant.EPOCH.getEpochSecond()),
        // not expired
        new Account.UsernameHold(TestRandomUtil.nextBytes(32),
            Instant.EPOCH.plus(Duration.ofSeconds(5)).getEpochSecond()));
    when(hasHolds.getUsernameHolds()).thenReturn(originalHolds);
    final Account noHolds = mock(Account.class);

    removeExpiredUsernameHoldsCommand.crawlAccounts(Flux.just(hasHolds, noHolds));

    if (isDryRun) {
      verifyNoInteractions(accountsManager);
    } else {
      ArgumentCaptor<Consumer<Account>> updaterCaptor = ArgumentCaptor.forClass(Consumer.class);
      verify(accountsManager, times(1)).updateAsync(eq(hasHolds), updaterCaptor.capture());
      final Consumer<Account> consumer = updaterCaptor.getValue();
      consumer.accept(hasHolds);
      verify(hasHolds, times(1)).setUsernameHolds(argThat(holds ->
          holds.equals(List.of(originalHolds.getLast()))));
      verifyNoMoreInteractions(accountsManager);
    }
  }

  @Test
  public void removeHolds() {
    final List<Account.UsernameHold> holds = IntStream.range(0, 100)
        .mapToObj(i -> new Account.UsernameHold(TestRandomUtil.nextBytes(32), i)).toList();
    final List<Account.UsernameHold> shuffled = new ArrayList<>(holds);
    Collections.shuffle(shuffled);

    final int currentTime = ThreadLocalRandom.current().nextInt(0, 100);
    final Clock clock = TestClock.pinned(Instant.EPOCH.plus(Duration.ofSeconds(currentTime)));
    final RemoveExpiredUsernameHoldsCommand removeExpiredUsernameHoldsCommand =
        new TestRemoveExpiredUsernameHoldsCommand(clock, mock(AccountsManager.class), false);

    final List<Account.UsernameHold> actual = new ArrayList<>(shuffled);
    final int numRemoved = removeExpiredUsernameHoldsCommand.removeExpired(actual);

    assertThat(numRemoved).isEqualTo(currentTime);
    assertThat(actual).hasSize(100 - currentTime);

    // should preserve order
    final Iterator<Account.UsernameHold> expected = shuffled.iterator();
    for (Account.UsernameHold hold : actual) {
      while (!Arrays.equals(expected.next().usernameHash(), hold.usernameHash())) {
        assertThat(expected).as("expected should be in order").hasNext();
      }
    }
  }
}
