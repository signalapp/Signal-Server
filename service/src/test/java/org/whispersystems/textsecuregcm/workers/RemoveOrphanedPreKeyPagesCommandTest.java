/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.dropwizard.core.setup.Environment;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import net.sourceforge.argparse4j.inf.Namespace;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.storage.DeviceKEMPreKeyPages;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import org.whispersystems.textsecuregcm.util.TestClock;
import reactor.core.publisher.Flux;

public class RemoveOrphanedPreKeyPagesCommandTest {


  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  public void removeStalePages(boolean dryRun) throws Exception {
    final TestClock clock = TestClock.pinned(Instant.EPOCH.plus(Duration.ofSeconds(10)));
    final KeysManager keysManager = mock(KeysManager.class);

    final UUID currentPage = UUID.randomUUID();
    final UUID freshOrphanedPage = UUID.randomUUID();
    final UUID staleOrphanedPage = UUID.randomUUID();

    when(keysManager.listStoredKEMPreKeyPages(anyInt())).thenReturn(Flux.fromIterable(List.of(
        new DeviceKEMPreKeyPages(UUID.randomUUID(), (byte) 1, Optional.of(currentPage), Map.of(
            currentPage, Instant.EPOCH,
            staleOrphanedPage, Instant.EPOCH.plus(Duration.ofSeconds(4)),
            freshOrphanedPage, Instant.EPOCH.plus(Duration.ofSeconds(5)))))));

    when(keysManager.pruneDeadPage(any(), anyByte(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    runCommand(clock, Duration.ofSeconds(5), dryRun, keysManager);
    verify(keysManager, times(dryRun ? 0 : 1))
        .pruneDeadPage(any(), eq((byte) 1), eq(staleOrphanedPage));
    verify(keysManager, times(1)).listStoredKEMPreKeyPages(anyInt());
    verifyNoMoreInteractions(keysManager);
  }

  @Test
  public void noCurrentPage() throws Exception {
    final TestClock clock = TestClock.pinned(Instant.EPOCH.plus(Duration.ofSeconds(10)));
    final KeysManager keysManager = mock(KeysManager.class);

    final UUID freshOrphanedPage = UUID.randomUUID();
    final UUID staleOrphanedPage = UUID.randomUUID();

    when(keysManager.listStoredKEMPreKeyPages(anyInt())).thenReturn(Flux.fromIterable(List.of(
        new DeviceKEMPreKeyPages(UUID.randomUUID(), (byte) 1, Optional.empty(), Map.of(
            staleOrphanedPage, Instant.EPOCH.plus(Duration.ofSeconds(4)),
            freshOrphanedPage, Instant.EPOCH.plus(Duration.ofSeconds(5)))))));

    when(keysManager.pruneDeadPage(any(), anyByte(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    runCommand(clock, Duration.ofSeconds(5), false, keysManager);
    verify(keysManager, times(1))
        .pruneDeadPage(any(), eq((byte) 1), eq(staleOrphanedPage));
    verify(keysManager, times(1)).listStoredKEMPreKeyPages(anyInt());
    verifyNoMoreInteractions(keysManager);
  }

  @Test
  public void noPages() throws Exception {
    final TestClock clock = TestClock.pinned(Instant.EPOCH);
    final KeysManager keysManager = mock(KeysManager.class);
    when(keysManager.listStoredKEMPreKeyPages(anyInt())).thenReturn(Flux.empty());
    runCommand(clock, Duration.ofSeconds(5), false, keysManager);
    verify(keysManager).listStoredKEMPreKeyPages(anyInt());
    verifyNoMoreInteractions(keysManager);
  }

  private enum PageStatus {NO_CURRENT, MATCH_CURRENT, MISMATCH_CURRENT}

  @CartesianTest
  void shouldDeletePage(
      @CartesianTest.Enum final PageStatus pageStatus,
      @CartesianTest.Values(booleans = {false, true}) final boolean isOld) {
    final Optional<UUID> currentPage = pageStatus == PageStatus.NO_CURRENT
        ? Optional.empty()
        : Optional.of(UUID.randomUUID());
    final UUID page = switch (pageStatus) {
      case MATCH_CURRENT -> currentPage.orElseThrow();
      case NO_CURRENT, MISMATCH_CURRENT -> UUID.randomUUID();
    };

    final Instant threshold = Instant.EPOCH.plus(Duration.ofSeconds(10));
    final Instant lastModified = isOld ? threshold.minus(Duration.ofSeconds(1)) : threshold;

    final boolean shouldDelete = pageStatus != PageStatus.MATCH_CURRENT && isOld;
    Assertions.assertThat(RemoveOrphanedPreKeyPagesCommand.shouldDeletePage(currentPage, page, threshold, lastModified))
        .isEqualTo(shouldDelete);
  }


  private void runCommand(final Clock clock, final Duration minimumOrphanAge, final boolean dryRun,
      final KeysManager keysManager) throws Exception {
    final CommandDependencies commandDependencies = mock(CommandDependencies.class);
    when(commandDependencies.keysManager()).thenReturn(keysManager);

    final Namespace namespace = mock(Namespace.class);
    when(namespace.getBoolean(RemoveOrphanedPreKeyPagesCommand.DRY_RUN_ARGUMENT)).thenReturn(dryRun);
    when(namespace.getInt(RemoveOrphanedPreKeyPagesCommand.CONCURRENCY_ARGUMENT)).thenReturn(2);
    when(namespace.getString(RemoveOrphanedPreKeyPagesCommand.MINIMUM_ORPHAN_AGE_ARGUMENT))
        .thenReturn(minimumOrphanAge.toString());

    final RemoveOrphanedPreKeyPagesCommand command = new RemoveOrphanedPreKeyPagesCommand(clock);
    command.run(mock(Environment.class), namespace, mock(WhisperServerConfiguration.class), commandDependencies);
  }
}
