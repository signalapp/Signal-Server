/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;


import static org.assertj.core.api.Assertions.assertThat;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;
import org.assertj.core.util.Streams;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.signal.libsignal.zkgroup.backups.BackupLevel;
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
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID);
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
    final AuthenticatedBackupUser backupUser = backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID);
    if (mediaAlreadyExists) {
      this.backupsDb.trackMedia(backupUser, 1, 10).join();
    }
    backupsDb.setMediaUsage(backupUser, new UsageInfo(113, 17)).join();
    final BackupsDb.TimestampedUsageInfo info = backupsDb.getMediaUsage(backupUser).join();
    assertThat(info.lastRecalculationTime()).isEqualTo(Instant.ofEpochSecond(5));
    assertThat(info.usageInfo().bytesUsed()).isEqualTo(113L);
    assertThat(info.usageInfo().numObjects()).isEqualTo(17L);
  }

  @Test
  public void expirationDetectedOnce() {
    final byte[] backupId = TestRandomUtil.nextBytes(16);
    // Refresh media/messages at t=0D
    testClock.pin(days(0));
    backupsDb.setPublicKey(backupId, BackupLevel.PAID, ECKeyPair.generate().getPublicKey()).join();
    this.backupsDb.ttlRefresh(backupUser(backupId, BackupCredentialType.MEDIA, BackupLevel.PAID)).join();

    // refresh only messages on t=2D
    testClock.pin(days(2).plus(Duration.ofSeconds(123)));
    this.backupsDb.ttlRefresh(backupUser(backupId, BackupCredentialType.MEDIA, BackupLevel.FREE)).join();

    final Function<Instant, List<ExpiredBackup>> expiredBackups = purgeTime -> backupsDb
        .getExpiredBackups(1, Schedulers.immediate(), purgeTime)
        .collectList()
        .block();

    // the media should be expired at t=1D
    List<ExpiredBackup> expired = expiredBackups.apply(days(1));
    assertThat(expired).hasSize(1).first()
        .matches(eb -> eb.expirationType() == ExpiredBackup.ExpirationType.MEDIA);

    // Expire the media
    backupsDb.startExpiration(expired.getFirst()).join();
    backupsDb.finishExpiration(expired.getFirst()).join();

    // should be nothing left to expire at t=1D
    assertThat(expiredBackups.apply(days(1))).isEmpty();

    // at t=3D, should now expire messages as well
    expired = expiredBackups.apply(days(3));
    assertThat(expired).hasSize(1).first()
        .matches(eb -> eb.expirationType() == ExpiredBackup.ExpirationType.ALL);

    // Expire the messages
    backupsDb.startExpiration(expired.getFirst()).join();
    backupsDb.finishExpiration(expired.getFirst()).join();

    // should be nothing to expire at t=3
    assertThat(expiredBackups.apply(days(3))).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(names = {"MEDIA", "ALL"})
  public void expirationFailed(ExpiredBackup.ExpirationType expirationType) {
    final byte[] backupId = TestRandomUtil.nextBytes(16);
    // Refresh media/messages at t=0D
    testClock.pin(days(0));
    backupsDb.setPublicKey(backupId, BackupLevel.PAID, ECKeyPair.generate().getPublicKey()).join();
    this.backupsDb.ttlRefresh(backupUser(backupId, BackupCredentialType.MEDIA, BackupLevel.PAID)).join();

    if (expirationType == ExpiredBackup.ExpirationType.MEDIA) {
      // refresh only messages at t=2D so that we only expire media at t=1D
      testClock.pin(days(2));
      this.backupsDb.ttlRefresh(backupUser(backupId, BackupCredentialType.MEDIA, BackupLevel.FREE)).join();
    }

    final Function<Instant, Optional<ExpiredBackup>> expiredBackups = purgeTime -> {
      final List<ExpiredBackup> res = backupsDb
          .getExpiredBackups(1, Schedulers.immediate(), purgeTime)
          .collectList()
          .block();
      assertThat(res).hasSizeLessThanOrEqualTo(1);
      return res.stream().findFirst();
    };

    BackupsDb.AuthenticationData info = backupsDb.retrieveAuthenticationData(backupId).join().get();
    final String originalBackupDir = info.backupDir();
    final String originalMediaDir = info.mediaDir();

    ExpiredBackup expired = expiredBackups.apply(days(1)).get();
    assertThat(expired).matches(eb -> eb.expirationType() == expirationType);

    // expire but fail (don't call finishExpiration)
    backupsDb.startExpiration(expired).join();
    info = backupsDb.retrieveAuthenticationData(backupId).join().get();
    if (expirationType == ExpiredBackup.ExpirationType.MEDIA) {
      // Media expiration should swap the media name and keep the backup name, marking the old media name for expiration
      assertThat(expired.prefixToDelete())
          .withFailMessage("Should expire media directory, expired %s", expired.prefixToDelete())
          .isEqualTo(originalBackupDir + "/" + originalMediaDir);
      assertThat(info.backupDir()).withFailMessage("should keep backupDir").isEqualTo(originalBackupDir);
      assertThat(info.mediaDir()).withFailMessage("should change mediaDir").isNotEqualTo(originalMediaDir);
    } else {
      // Full expiration should swap the media name and the backup name, marking the old backup name for expiration
      assertThat(expired.prefixToDelete())
          .withFailMessage("Should expire whole backupDir, expired %s", expired.prefixToDelete())
          .isEqualTo(originalBackupDir);
      assertThat(info.backupDir()).withFailMessage("should change backupDir").isNotEqualTo(originalBackupDir);
      assertThat(info.mediaDir()).withFailMessage("should change mediaDir").isNotEqualTo(originalMediaDir);
    }
    final String expiredPrefix = expired.prefixToDelete();

    // We failed, so we should see the same prefix on the next expiration listing
    expired = expiredBackups.apply(days(1)).get();
    assertThat(expired).matches(eb -> eb.expirationType() == ExpiredBackup.ExpirationType.GARBAGE_COLLECTION,
        "Expiration should be garbage collection ");
    assertThat(expired.prefixToDelete()).isEqualTo(expiredPrefix);
    backupsDb.startExpiration(expired).join();

    // Successfully finish the expiration
    backupsDb.finishExpiration(expired).join();

    Optional<ExpiredBackup> opt = expiredBackups.apply(days(1));
    if (expirationType == ExpiredBackup.ExpirationType.MEDIA) {
      // should be nothing to expire at t=1
      assertThat(opt).isEmpty();
      // The backup should still exist
      backupsDb.describeBackup(backupUser(backupId, BackupCredentialType.MEDIA, BackupLevel.PAID)).join();
    } else {
      // Cleaned up the failed attempt, now should tell us to clean the whole backup
      assertThat(opt.get()).matches(eb -> eb.expirationType() == ExpiredBackup.ExpirationType.ALL,
          "Expiration should be all ");
      backupsDb.startExpiration(opt.get()).join();
      backupsDb.finishExpiration(opt.get()).join();

      // The backup entry should be gone
      assertThat(CompletableFutureTestUtil.assertFailsWithCause(StatusRuntimeException.class,
              backupsDb.describeBackup(backupUser(backupId, BackupCredentialType.MEDIA, BackupLevel.PAID)))
          .getStatus().getCode())
          .isEqualTo(Status.Code.NOT_FOUND);
      assertThat(expiredBackups.apply(Instant.ofEpochSecond(10))).isEmpty();
    }
  }

  @Test
  public void list() {
    final List<AuthenticatedBackupUser> users = List.of(
        backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.FREE),
        backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID),
        backupUser(TestRandomUtil.nextBytes(16), BackupCredentialType.MEDIA, BackupLevel.PAID));

    final List<Instant> lastRefreshTimes = List.of(
        days(1).plus(Duration.ofSeconds(12)),
        days(2).plus(Duration.ofSeconds(34)),
        days(3).plus(Duration.ofSeconds(56)));

    // add at least one message backup, so we can describe it
    for (int i = 0; i < users.size(); i++) {
      testClock.pin(lastRefreshTimes.get(i));
      backupsDb.addMessageBackup(users.get(i)).join();
    }

    backupsDb.trackMedia(users.get(1), 10, 100).join();
    backupsDb.trackMedia(users.get(2), 1, 1000).join();

    final List<StoredBackupAttributes> sbms = backupsDb.listBackupAttributes(1, Schedulers.immediate())
        .sort(Comparator.comparing(StoredBackupAttributes::lastRefresh))
        .collectList()
        .block();

    final StoredBackupAttributes sbm1 = sbms.get(0);
    assertThat(sbm1.bytesUsed()).isEqualTo(0);
    assertThat(sbm1.numObjects()).isEqualTo(0);
    assertThat(sbm1.lastRefresh()).isEqualTo(lastRefreshTimes.get(0).truncatedTo(ChronoUnit.DAYS));
    assertThat(sbm1.lastMediaRefresh()).isEqualTo(Instant.EPOCH);


    final StoredBackupAttributes sbm2 = sbms.get(1);
    assertThat(sbm2.bytesUsed()).isEqualTo(100);
    assertThat(sbm2.numObjects()).isEqualTo(10);
    assertThat(sbm2.lastRefresh()).isEqualTo(lastRefreshTimes.get(1).truncatedTo(ChronoUnit.DAYS));
    assertThat(sbm2.lastMediaRefresh()).isEqualTo(lastRefreshTimes.get(1).truncatedTo(ChronoUnit.DAYS));

    final StoredBackupAttributes sbm3 = sbms.get(2);
    assertThat(sbm3.bytesUsed()).isEqualTo(1000);
    assertThat(sbm3.numObjects()).isEqualTo(1);
    assertThat(sbm3.lastRefresh()).isEqualTo(lastRefreshTimes.get(2).truncatedTo(ChronoUnit.DAYS));
    assertThat(sbm3.lastMediaRefresh()).isEqualTo(lastRefreshTimes.get(2).truncatedTo(ChronoUnit.DAYS));
  }

  private static Instant days(int n) {
    return Instant.EPOCH.plus(Duration.ofDays(n));
  }

  private AuthenticatedBackupUser backupUser(final byte[] backupId, final BackupCredentialType credentialType, final BackupLevel backupLevel) {
    return new AuthenticatedBackupUser(backupId, credentialType, backupLevel, "myBackupDir", "myMediaDir", null);
  }
}
