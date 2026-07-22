/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.util.DataSize;
import io.micrometer.core.instrument.Metrics;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.util.Pair;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TrimOversizedFoundationDbMessageQueuesCommand extends AbstractSinglePassCrawlAccountsCommand {
  @VisibleForTesting
  static final String DRY_RUN_ARGUMENT = "dry-run";

  @VisibleForTesting
  static final String MAX_CONCURRENCY_ARGUMENT = "max-concurrency";

  @VisibleForTesting
  static final String MAX_QUEUE_SIZE_BYTES_ARGUMENT = "max-queue-size-bytes";

  @VisibleForTesting
  static final String TARGET_QUEUE_SIZE_BYTES_ARGUMENT = "target-queue-size-bytes";

  @VisibleForTesting
  static final String RANGE_SPLIT_CHUNK_SIZE_BYTES_ARGUMENT = "range-split-chunk-size-bytes";

  @VisibleForTesting
  static final long DEFAULT_MAX_QUEUE_SIZE_BYTES = DataSize.gigabytes(10).toBytes();

  @VisibleForTesting
  static final long DEFAULT_TARGET_QUEUE_SIZE_BYTES = DataSize.gigabytes(9).toBytes();

  @VisibleForTesting
  static final long DEFAULT_RANGE_SPLIT_CHUNK_SIZE_BYTES = DataSize.gigabytes(1).toBytes();

  private static final String QUEUES_INSPECTED_COUNTER_NAME = MetricsUtil.name(
      TrimOversizedFoundationDbMessageQueuesCommand.class, "trimmedQueues");
  private static final int DEFAULT_MAX_CONCURRENCY = 16;
  private static final Logger LOGGER = LoggerFactory.getLogger(TrimOversizedFoundationDbMessageQueuesCommand.class);

  public TrimOversizedFoundationDbMessageQueuesCommand() {
    super("trim-oversized-fdb-message-queues", "Trims oversized message queues in FoundationDB");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don't actually trim any message queues.");

    subparser.addArgument("--max-concurrency")
        .type(Integer.class)
        .dest(MAX_CONCURRENCY_ARGUMENT)
        .required(false)
        .setDefault(DEFAULT_MAX_CONCURRENCY)
        .help("Max concurrency for FoundationDB operations");

    subparser.addArgument("--max-queue-size-bytes")
        .type(Long.class)
        .dest(MAX_QUEUE_SIZE_BYTES_ARGUMENT)
        .required(false)
        .setDefault(DEFAULT_MAX_QUEUE_SIZE_BYTES)
        .help("If a message queue size exceeds this limit, trim it down.");

    subparser.addArgument("--target-queue-size-bytes")
        .type(Long.class)
        .dest(TARGET_QUEUE_SIZE_BYTES_ARGUMENT)
        .required(false)
        .setDefault(DEFAULT_TARGET_QUEUE_SIZE_BYTES)
        .help("If trimming a message queue, the queue size to target.");

    subparser.addArgument("--range-split-chunk-size-bytes")
        .type(Long.class)
        .dest(RANGE_SPLIT_CHUNK_SIZE_BYTES_ARGUMENT)
        .required(false)
        .setDefault(DEFAULT_RANGE_SPLIT_CHUNK_SIZE_BYTES)
        .help("""
            The chunk size to use when determining split points for the range of keys covered by a message queue.
            FoundationDB does not provide any documentation on this parameter, but observationally,
            setting this value greater than 1 MB should be okay.
            """);
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {
    final long maxQueueSizeBytes = getNamespace().getLong(MAX_QUEUE_SIZE_BYTES_ARGUMENT);
    final long targetQueueSizeBytes = getNamespace().getLong(TARGET_QUEUE_SIZE_BYTES_ARGUMENT);
    final long rangeSplitChunkSizeBytes = getNamespace().getLong(RANGE_SPLIT_CHUNK_SIZE_BYTES_ARGUMENT);

    if (targetQueueSizeBytes <= 0) {
      throw new IllegalArgumentException("target-queue-size-bytes must be positive");
    }

    if (targetQueueSizeBytes >= maxQueueSizeBytes) {
      throw new IllegalArgumentException("target-queue-size-bytes must be less than max-queue-size-bytes");
    }

    if (rangeSplitChunkSizeBytes <= 0) {
      throw new IllegalArgumentException("range-split-chunk-size-bytes must be positive");
    }

    final boolean dryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);
    final int maxConcurrency = getNamespace().getInt(MAX_CONCURRENCY_ARGUMENT);

    final MessagesManager messagesManager = getCommandDependencies().messagesManager();

    accounts
        .flatMapIterable(account ->
            account.getDevices().stream().map(
                device -> new Pair<>(new AciServiceIdentifier(account.getIdentifier(IdentityType.ACI)), device)).toList())
        .doOnNext(_ -> Metrics.counter(QUEUES_INSPECTED_COUNTER_NAME, "dryRun", String.valueOf(dryRun)).increment())
        .flatMap(accountAndDevice ->
            messagesManager.trimQueue(accountAndDevice.first(), accountAndDevice.second(), maxQueueSizeBytes, targetQueueSizeBytes, rangeSplitChunkSizeBytes, dryRun),
            maxConcurrency)
        .onErrorResume(e  -> {
          LOGGER.error("Failed to trim queue", e);
          return Mono.empty();
        })
        .then()
        .block();
  }
}
