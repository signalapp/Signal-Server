/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.stream.Collectors;
import net.sourceforge.argparse4j.inf.Subparser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import reactor.core.publisher.Flux;

public class ClearExpiredFoundationDbMessagesCommand extends AbstractSinglePassCrawlAccountsCommand {

  private final Clock clock;

  @VisibleForTesting
  static final String DRY_RUN_ARGUMENT = "dry-run";

  @VisibleForTesting
  static final String BATCH_SIZE_ARGUMENT = "batch-size";

  @VisibleForTesting
  static final String MESSAGE_TTL_DAYS_ARGUMENT = "message-ttl-days";

  @VisibleForTesting
  static final String VERSIONSTAMP_TTL_DAYS_ARGUMENT = "versionstamp-ttl-days";

  @VisibleForTesting
  static final Duration TIMESTAMP_RETENTION_PERIOD = Duration.ofDays(60);

  private static final Logger log = LoggerFactory.getLogger(ClearExpiredFoundationDbMessagesCommand.class);

  public ClearExpiredFoundationDbMessagesCommand(final Clock clock) {
    super("clear-expired-foundationdb-messages", "Removes expired messages from FoundationDB");

    this.clock = clock;
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--dry-run")
        .type(Boolean.class)
        .dest(DRY_RUN_ARGUMENT)
        .required(false)
        .setDefault(true)
        .help("If true, don't actually delete any messages (but do still make entries in the versionstamp clock)");

    subparser.addArgument("--batch-size")
        .type(Integer.class)
        .dest(BATCH_SIZE_ARGUMENT)
        .required(false)
        .setDefault(10000)
        .help("Maximum number of queues to trim in a single transaction");

    subparser.addArgument("--message-ttl-days")
        .type(Integer.class)
        .dest(MESSAGE_TTL_DAYS_ARGUMENT)
        .required(true)
        .help("Desired message TTL in days");

    subparser.addArgument("--versionstamp-ttl-days")
        .type(Integer.class)
        .dest(VERSIONSTAMP_TTL_DAYS_ARGUMENT)
        .required(false)
        .setDefault(180)
        .help("Desired versionstamp-clock TTL in days");
  }

  @Override
  protected void crawlAccounts(final Flux<Account> accounts) {
    final MessagesManager messagesManager = getCommandDependencies().messagesManager();

    // Record the current versionstamp/time pair for the benefit of future runs. Because this job runs regularly, it has
    // the dual role of clearing messages and keeping the versionstamp/time mappings populated.
    messagesManager.recordFoundationDbVersionstamps();

    final int batchSize = getNamespace().getInt(BATCH_SIZE_ARGUMENT);
    final boolean dryRun = getNamespace().getBoolean(DRY_RUN_ARGUMENT);
    final int messageTtlDays = getNamespace().getInt(MESSAGE_TTL_DAYS_ARGUMENT);
    final int versionstampTtlDays = getNamespace().getInt(VERSIONSTAMP_TTL_DAYS_ARGUMENT);

    final Instant oldestMessageTime = clock.instant().minus(Duration.ofDays(messageTtlDays));
    final Instant oldestVersionstampTime = clock.instant().minus(Duration.ofDays(versionstampTtlDays));

    accounts
        .buffer(batchSize)
        .map(batch -> batch.stream().collect(
            Collectors.toMap(
                account -> new AciServiceIdentifier(account.getIdentifier(IdentityType.ACI)),
                account -> account.getDevices().stream().map(Device::getId).toList())))
        .doOnNext(
            dryRun ? _ -> {}
                : deviceIdsByAccountIdentifier -> messagesManager.deleteFoundationDbMessagesBefore(deviceIdsByAccountIdentifier, oldestMessageTime))
        .then()
        .block();

    messagesManager.expireOldFoundationDbVersionstamps(oldestVersionstampTime);
  }
}
