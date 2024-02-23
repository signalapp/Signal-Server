/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;


import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import reactor.core.scheduler.Schedulers;

public class BackupsDbTest {

  @RegisterExtension
  public static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      DynamoDbExtensionSchema.Tables.BACKUPS);

  private final TestClock testClock = TestClock.now();
  private BackupsDb backupsDb;

  @BeforeEach
  public void setup() {
    testClock.unpin();
    backupsDb = new BackupsDb(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.BACKUPS.tableName(),
        testClock);
  }

  @Test
  public void trackMediaStats() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupTier.MEDIA);
    // add at least one message backup so we can describe it
    backupsDb.addMessageBackup(backupUser).join();
    int total = 0;
    for (int i = 0; i < 5; i++) {
      this.backupsDb.trackMedia(backupUser, 1, i).join();
      total += i;
      final BackupsDb.BackupDescription description = this.backupsDb.describeBackup(backupUser).join();
      assertThat(description.mediaUsedSpace().get()).isEqualTo(total);
    }

    for (int i = 0; i < 5; i++) {
      this.backupsDb.trackMedia(backupUser, -1, -i).join();
      total -= i;
      final BackupsDb.BackupDescription description = this.backupsDb.describeBackup(backupUser).join();
      assertThat(description.mediaUsedSpace().get()).isEqualTo(total);
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  public void setUsage(boolean mediaAlreadyExists) {
    testClock.pin(Instant.ofEpochSecond(5));
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupTier.MEDIA);
    if (mediaAlreadyExists) {
      this.backupsDb.trackMedia(backupUser, 1, 10).join();
    }
    backupsDb.setMediaUsage(backupUser, new UsageInfo( 113, 17)).join();
    final BackupsDb.TimestampedUsageInfo info = backupsDb.getMediaUsage(backupUser).join();
    assertThat(info.lastRecalculationTime()).isEqualTo(Instant.ofEpochSecond(5));
    assertThat(info.usageInfo().bytesUsed()).isEqualTo(113L);
    assertThat(info.usageInfo().numObjects()).isEqualTo(17L);
  }

  @Test
  public void expirationDetectedOnce() {
    final byte[] backupId = TestRandomUtil.nextBytes(16);
    // Refresh media/messages at t=0
    testClock.pin(Instant.ofEpochSecond(0L));
    this.backupsDb.ttlRefresh(backupUser(backupId, BackupTier.MEDIA)).join();

    // refresh only messages at t=2
    testClock.pin(Instant.ofEpochSecond(2L));
    this.backupsDb.ttlRefresh(backupUser(backupId, BackupTier.MESSAGES)).join();

    final Function<Instant, List<ExpiredBackup>> expiredBackups = purgeTime -> backupsDb
        .getExpiredBackups(1, Schedulers.immediate(), purgeTime)
        .collectList()
        .block();

    List<ExpiredBackup> expired = expiredBackups.apply(Instant.ofEpochSecond(1));
    assertThat(expired).hasSize(1).first()
        .matches(eb -> eb.backupTierToRemove() == BackupTier.MEDIA);

    // Expire the media
    backupsDb.clearMediaUsage(expired.get(0).hashedBackupId()).join();

    // should be nothing to expire at t=1
    assertThat(expiredBackups.apply(Instant.ofEpochSecond(1))).isEmpty();

    // at t=3, should now expire messages as well
    expired = expiredBackups.apply(Instant.ofEpochSecond(3));
    assertThat(expired).hasSize(1).first()
        .matches(eb -> eb.backupTierToRemove() == BackupTier.MESSAGES);

    // Expire the messages
    backupsDb.deleteBackup(expired.get(0).hashedBackupId()).join();

    // should be nothing to expire at t=3
    assertThat(expiredBackups.apply(Instant.ofEpochSecond(3))).isEmpty();
  }

  private AuthenticatedBackupUser backupUser(final byte[] backupId, final BackupTier backupTier) {
    return new AuthenticatedBackupUser(backupId, backupTier);
  }
}
