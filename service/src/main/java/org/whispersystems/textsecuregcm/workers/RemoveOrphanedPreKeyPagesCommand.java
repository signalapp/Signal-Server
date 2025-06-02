/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Environment;
import io.micrometer.core.instrument.Metrics;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.DeviceKEMPreKeyPages;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class RemoveOrphanedPreKeyPagesCommand extends AbstractCommandWithDependencies {

  private final Logger logger = LoggerFactory.getLogger(getClass());

  private static final String PAGE_CONSIDERED_COUNTER_NAME = MetricsUtil.name(RemoveOrphanedPreKeyPagesCommand.class,
      "pageConsidered");

  @VisibleForTesting
  static final String DRY_RUN_ARGUMENT = "dry-run";

  @VisibleForTesting
  static final String CONCURRENCY_ARGUMENT = "concurrency";
  private static final int DEFAULT_CONCURRENCY = 10;

  @VisibleForTesting
  static final String MINIMUM_ORPHAN_AGE_ARGUMENT = "orphan-age";
  private static final Duration DEFAULT_MINIMUM_ORPHAN_AGE = Duration.ofDays(7);



  private final Clock clock;

  public RemoveOrphanedPreKeyPagesCommand(final Clock clock) {
    super(new Application<>() {
      @Override
      public void run(final WhisperServerConfiguration configuration, final Environment environment) {
      }
    }, "remove-orphaned-pre-key-pages", "Remove pre-key pages that are unreferenced");
    this.clock = clock;
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--concurrency")
        .type(Integer.class)
        .dest(CONCURRENCY_ARGUMENT)
        .required(false)
        .setDefault(DEFAULT_CONCURRENCY)
        .help("The maximum number of parallel dynamodb operations to process concurrently");

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don't actually remove orphaned pre-key pages");

    subparser.addArgument("--minimum-orphan-age")
        .type(String.class)
        .dest(MINIMUM_ORPHAN_AGE_ARGUMENT)
        .required(false)
        .setDefault(DEFAULT_MINIMUM_ORPHAN_AGE.toString())
        .help("Only remove orphans that are at least this old. Provide as an ISO-8601 duration string");
  }

  @Override
  protected void run(final Environment environment, final Namespace namespace,
      final WhisperServerConfiguration configuration, final CommandDependencies commandDependencies) throws Exception {

    final int concurrency = Objects.requireNonNull(namespace.getInt(CONCURRENCY_ARGUMENT));
    final boolean dryRun = Objects.requireNonNull(namespace.getBoolean(DRY_RUN_ARGUMENT));
    final Duration orphanAgeMinimum =
        Duration.parse(Objects.requireNonNull(namespace.getString(MINIMUM_ORPHAN_AGE_ARGUMENT)));
    final Instant olderThan = clock.instant().minus(orphanAgeMinimum);

    logger.info("Crawling preKey page store with concurrency={}, processors={}, dryRun={}. Removing orphans written before={}",
        concurrency,
        Runtime.getRuntime().availableProcessors(),
        dryRun,
        olderThan);

    final KeysManager keysManager = commandDependencies.keysManager();
    final int deletedPages = keysManager.listStoredKEMPreKeyPages(concurrency)
        .flatMap(storedPages -> Flux.fromStream(getDetetablePages(storedPages, olderThan))
            .concatMap(pageId -> dryRun
                ? Mono.just(0)
                : Mono.fromCompletionStage(() ->
                        keysManager.pruneDeadPage(storedPages.identifier(), storedPages.deviceId(), pageId))
                    .retryWhen(Retry.backoff(3, Duration.ofSeconds(1)))
                    .thenReturn(1)), concurrency)
        .reduce(0, Integer::sum)
        .block();
    logger.info("Deleted {} orphaned pages", deletedPages);
  }

  private static Stream<UUID> getDetetablePages(final DeviceKEMPreKeyPages storedPages, final Instant olderThan) {
    return storedPages.pageIdToLastModified()
        .entrySet()
        .stream()
        .filter(page -> {
          final UUID pageId = page.getKey();
          final Instant lastModified = page.getValue();
          return shouldDeletePage(storedPages.currentPage(), pageId, olderThan, lastModified);
        })
        .map(Map.Entry::getKey);
  }

  @VisibleForTesting
  static boolean shouldDeletePage(
      final Optional<UUID> currentPage, final UUID page,
      final Instant deleteBefore, final Instant lastModified) {
    final boolean isCurrentPageForDevice = currentPage.map(uuid -> uuid.equals(page)).orElse(false);
    final boolean isStale = lastModified.isBefore(deleteBefore);
    Metrics.counter(PAGE_CONSIDERED_COUNTER_NAME,
            "isCurrentPageForDevice", Boolean.toString(isCurrentPageForDevice),
            "stale", Boolean.toString(isStale))
        .increment();
    return !isCurrentPageForDevice && isStale;
  }
}
