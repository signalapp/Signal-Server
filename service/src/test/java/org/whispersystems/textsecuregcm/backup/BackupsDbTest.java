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
import java.util.Arrays;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema;
import org.whispersystems.textsecuregcm.util.CompletableFutureTestUtil;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

public class BackupsDbTest {

  @RegisterExtension
  public static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(
      DynamoDbExtensionSchema.Tables.BACKUPS,
      DynamoDbExtensionSchema.Tables.BACKUP_MEDIA);

  private final TestClock testClock = TestClock.now();
  private BackupsDb backupsDb;

  @BeforeEach
  public void setup() {
    testClock.unpin();
    backupsDb = new BackupsDb(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.BACKUPS.tableName(), DynamoDbExtensionSchema.Tables.BACKUP_MEDIA.tableName(),
        testClock);
  }

  @Test
  public void trackMediaIdempotent() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupTier.MEDIA);
    this.backupsDb.trackMedia(backupUser, "abc".getBytes(StandardCharsets.UTF_8), 100).join();
    assertDoesNotThrow(() ->
        this.backupsDb.trackMedia(backupUser, "abc".getBytes(StandardCharsets.UTF_8), 100).join());
  }

  @Test
  public void trackMediaLengthChange() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupTier.MEDIA);
    this.backupsDb.trackMedia(backupUser, "abc".getBytes(StandardCharsets.UTF_8), 100).join();
    CompletableFutureTestUtil.assertFailsWithCause(InvalidLengthException.class,
        this.backupsDb.trackMedia(backupUser, "abc".getBytes(StandardCharsets.UTF_8), 99));
  }

  @Test
  public void trackMediaStats() {
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupTier.MEDIA);
    // add at least one message backup so we can describe it
    backupsDb.addMessageBackup(backupUser).join();
    int total = 0;
    for (int i = 0; i < 5; i++) {
      this.backupsDb.trackMedia(backupUser, Integer.toString(i).getBytes(StandardCharsets.UTF_8), i).join();
      total += i;
      final BackupsDb.BackupDescription description = this.backupsDb.describeBackup(backupUser).join();
      assertThat(description.mediaUsedSpace().get()).isEqualTo(total);
    }

    for (int i = 0; i < 5; i++) {
      this.backupsDb.untrackMedia(backupUser, Integer.toString(i).getBytes(StandardCharsets.UTF_8), i).join();
      total -= i;
      final BackupsDb.BackupDescription description = this.backupsDb.describeBackup(backupUser).join();
      assertThat(description.mediaUsedSpace().get()).isEqualTo(total);
    }
  }


  private static byte[] hashedBackupId(final byte[] backupId) {
    try {
      return Arrays.copyOf(MessageDigest.getInstance("SHA-256").digest(backupId), 16);
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  private AuthenticatedBackupUser backupUser(final byte[] backupId, final BackupTier backupTier) {
    return new AuthenticatedBackupUser(backupId, backupTier);
  }
}
