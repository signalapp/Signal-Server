/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.backup;

import io.grpc.Status;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Predicate;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.zkgroup.backups.BackupLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.Update;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * Tracks backup metadata in a persistent store.
 * <p>
 * It's assumed that the caller has already validated that the backupUser being operated on has valid credentials and
 * possesses the appropriate {@link BackupLevel} to perform the current operation.
 * <p>
 * Backup records track two timestamps indicating the last time that a user interacted with their backup. One for the
 * last refresh that contained a credential including media level, and the other for any access. After a period of
 * inactivity stale backups can be purged (either just the media, or the entire backup). Callers can discover what
 * backups are stale and whether only the media or the entire backup is stale via {@link #getExpiredBackups}.
 * <p>
 * Because backup objects reside on a transactionally unrelated store, expiring anything from the backup requires a 2
 * phase process. First the caller calls {@link #startExpiration} which will atomically update the user's backup
 * directories and record the cdn directory that should be expired. Then the caller must delete the expired directory,
 * calling {@link #finishExpiration} to clear the recorded expired prefix when complete. Since the user's backup
 * directories have been swapped, the deleter does not have to account for a user coming back and starting to upload
 * concurrently with the deletion.
 * <p>
 * If the directory deletion fails, a subsequent call to {@link #getExpiredBackups} will return the backup again
 * indicating that the old expired prefix needs to be cleaned up before any other expiration action is taken. For
 * example, if a media expiration fails and then in the next expiration pass the backup has become eligible for total
 * deletion, the caller still must process the stale media expiration first before processing the full deletion.
 */
public class BackupsDb {

  private static final int DIR_NAME_LENGTH = generateDirName(new SecureRandom()).length();
  public static final int BACKUP_DIRECTORY_PATH_LENGTH = DIR_NAME_LENGTH;
  public static final int MEDIA_DIRECTORY_PATH_LENGTH = BACKUP_DIRECTORY_PATH_LENGTH + "/".length() + DIR_NAME_LENGTH;
  private static final Logger logger = LoggerFactory.getLogger(BackupsDb.class);
  static final int BACKUP_CDN = 3;

  private final DynamoDbAsyncClient dynamoClient;
  private final String backupTableName;
  private final Clock clock;

  private final SecureRandom secureRandom;

  private static final String NUM_OBJECTS_SUMMARY_NAME = MetricsUtil.name(BackupsDb.class, "numObjects");
  private static final String BYTES_USED_SUMMARY_NAME = MetricsUtil.name(BackupsDb.class, "bytesUsed");
  private static final String BACKUPS_COUNTER_NAME = MetricsUtil.name(BackupsDb.class, "backups");

  // The backups table

  // B: 16 bytes that identifies the backup
  public static final String KEY_BACKUP_ID_HASH = "U";
  // N: Time in seconds since epoch of the last backup refresh. This timestamp must be periodically updated to avoid
  // garbage collection of archive objects.
  public static final String ATTR_LAST_REFRESH = "R";
  // N: Time in seconds since epoch of the last backup media refresh. This timestamp can only be updated if the client
  // has BackupLevel.PAID, and must be periodically updated to avoid garbage collection of media objects.
  public static final String ATTR_LAST_MEDIA_REFRESH = "MR";
  // B: A 32 byte public key that should be used to sign the presentation used to authenticate requests against the
  // backup-id
  public static final String ATTR_PUBLIC_KEY = "P";
  // N: Bytes consumed by this backup
  public static final String ATTR_MEDIA_BYTES_USED = "MB";
  // N: Number of media objects in the backup
  public static final String ATTR_MEDIA_COUNT = "MC";
  // N: The cdn number where the message backup is stored
  public static final String ATTR_CDN = "CDN";
  // N: Time in seconds since epoch of last backup media usage recalculation. This timestamp is updated whenever we
  // recalculate the up-to-date bytes used by querying the cdn(s) directly.
  public static final String ATTR_MEDIA_USAGE_LAST_RECALCULATION = "MBTS";
  // S: The name of the user's backup directory on the CDN
  public static final String ATTR_BACKUP_DIR = "BD";
  // S: The name of the user's media directory within the backup directory on the CDN
  public static final String ATTR_MEDIA_DIR = "MD";
  // S: A prefix pending deletion
  public static final String ATTR_EXPIRED_PREFIX = "EP";

  public BackupsDb(
      final DynamoDbAsyncClient dynamoClient,
      final String backupTableName,
      final Clock clock) {
    this.dynamoClient = dynamoClient;
    this.backupTableName = backupTableName;
    this.clock = clock;
    this.secureRandom = new SecureRandom();
  }

  /**
   * Set the public key associated with a backupId.
   *
   * @param authenticatedBackupId    The backup-id bytes that should be associated with the provided public key
   * @param authenticatedBackupLevel The backup level
   * @param publicKey                The public key to associate with the backup id
   * @return A stage that completes when the public key has been set. If the backup-id already has a set public key that
   * does not match, the stage will be completed exceptionally with a {@link PublicKeyConflictException}
   */
  CompletableFuture<Void> setPublicKey(
      final byte[] authenticatedBackupId,
      final BackupLevel authenticatedBackupLevel,
      final ECPublicKey publicKey) {
    final byte[] hashedBackupId = hashedBackupId(authenticatedBackupId);
    return dynamoClient.updateItem(new UpdateBuilder(backupTableName, authenticatedBackupLevel, hashedBackupId)
            .addSetExpression("#publicKey = :publicKey",
                Map.entry("#publicKey", ATTR_PUBLIC_KEY),
                Map.entry(":publicKey", AttributeValues.b(publicKey.serialize())))
            // When the user sets a public key, we ensure that they have a backupDir/mediaDir assigned
            .setDirectoryNamesIfMissing(secureRandom)
            .setRefreshTimes(clock)
            .withConditionExpression("attribute_not_exists(#publicKey) OR #publicKey = :publicKey")
            .updateItemBuilder()
            .build())
        .exceptionally(ExceptionUtils.marshal(ConditionalCheckFailedException.class, e ->
            // There was already a row for this backup-id and it contained a different publicKey
            new PublicKeyConflictException()))
        .thenRun(Util.NOOP);
  }

  /**
   * Data stored to authenticate a backup user
   *
   * @param publicKey The public key for the backup entry. All credentials for this backup user must be signed * by this
   *                  public key for the credential to be valid
   * @param backupDir The current backupDir for the backup user. If authentication is successful, the user may be given
   *                  credentials for this backupDir on the CDN
   * @param mediaDir  The current mediaDir for the backup user. If authentication is successful, the user may be given *
   *                  credentials for the path backupDir/mediaDir on the CDN
   */
  record AuthenticationData(ECPublicKey publicKey, String backupDir, String mediaDir) {}

  CompletableFuture<Optional<AuthenticationData>> retrieveAuthenticationData(byte[] backupId) {
    final byte[] hashedBackupId = hashedBackupId(backupId);
    return dynamoClient.getItem(GetItemRequest.builder()
            .tableName(backupTableName)
            .key(Map.of(KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId)))
            .consistentRead(true)
            .projectionExpression("#publicKey,#backupDir,#mediaDir")
            .expressionAttributeNames(Map.of(
                "#publicKey", ATTR_PUBLIC_KEY,
                "#backupDir", ATTR_BACKUP_DIR,
                "#mediaDir", ATTR_MEDIA_DIR))
            .build())
        .thenApply(response -> extractStoredPublicKey(response.item())
            .map(pubKey -> new AuthenticationData(
                pubKey,
                getDirName(response.item(), ATTR_BACKUP_DIR),
                getDirName(response.item(), ATTR_MEDIA_DIR))));
  }

  private static String getDirName(final Map<String, AttributeValue> item, final String attr) {
    return AttributeValues.get(item, attr).map(AttributeValue::s).orElseThrow(() -> {
      logger.error("Backups with public keys should have directory names");
      return Status.INTERNAL
          .withDescription("Backups with public keys must have directory names")
          .asRuntimeException();
    });
  }

  private static Optional<ECPublicKey> extractStoredPublicKey(final Map<String, AttributeValue> item) {
    return AttributeValues.get(item, ATTR_PUBLIC_KEY)
        .map(AttributeValue::b)
        .map(SdkBytes::asByteArray)
        .map(BackupsDb::deserializeStoredPublicKey);
  }

  private static ECPublicKey deserializeStoredPublicKey(final byte[] publicKeyBytes) {
    try {
      return new ECPublicKey(publicKeyBytes);
    } catch (InvalidKeyException e) {
      logger.error("Invalid publicKey {}", HexFormat.of().formatHex(publicKeyBytes), e);
      throw Status.INTERNAL
          .withCause(e)
          .withDescription("Could not deserialize stored public key")
          .asRuntimeException();
    }
  }

  /**
   * Update the quota in the backup table
   *
   * @param backupUser      The backup user
   * @param mediaBytesDelta The length of the media after encryption. A negative length implies media being removed
   * @param mediaCountDelta The number of media objects being added, or if negative, removed
   * @return A stage that completes successfully once the table are updated.
   */
  CompletableFuture<Void> trackMedia(final AuthenticatedBackupUser backupUser, final long mediaCountDelta,
      final long mediaBytesDelta) {
    return dynamoClient
        .updateItem(
            // Update the media quota and TTL
            UpdateBuilder.forUser(backupTableName, backupUser)
                .incrementMediaBytes(mediaBytesDelta)
                .incrementMediaCount(mediaCountDelta)
                .updateItemBuilder()
                .build())
        .thenRun(Util.NOOP);
  }


  /**
   * Update the last update timestamps for the backupId in the presentation
   *
   * @param backupUser an already authorized backup user
   */
  CompletableFuture<Void> ttlRefresh(final AuthenticatedBackupUser backupUser) {
    final Instant today = clock.instant().truncatedTo(ChronoUnit.DAYS);
    // update message backup TTL
    return dynamoClient.updateItem(UpdateBuilder.forUser(backupTableName, backupUser)
            .setRefreshTimes(today)
            .updateItemBuilder()
            .returnValues(ReturnValue.ALL_OLD)
            .build())
        .thenAccept(updateItemResponse ->
            updateMetricsAfterRefresh(backupUser, today, updateItemResponse.attributes()));
  }

  /**
   * Track that a backup will be stored for the user
   *
   * @param backupUser an already authorized backup user
   */
  CompletableFuture<Void> addMessageBackup(final AuthenticatedBackupUser backupUser) {
    final Instant today = clock.instant().truncatedTo(ChronoUnit.DAYS);
    // this could race with concurrent updates, but the only effect would be last-writer-wins on the timestamp
    return dynamoClient.updateItem(
            UpdateBuilder.forUser(backupTableName, backupUser)
                .setRefreshTimes(today)
                .setCdn(BACKUP_CDN)
                .updateItemBuilder()
                .returnValues(ReturnValue.ALL_OLD)
                .build())
        .thenAccept(updateItemResponse ->
            updateMetricsAfterRefresh(backupUser, today, updateItemResponse.attributes()));
  }

  private void updateMetricsAfterRefresh(final AuthenticatedBackupUser backupUser, final Instant today, final Map<String, AttributeValue> item) {
    final Instant previousRefreshTime = Instant.ofEpochSecond(
        AttributeValues.getLong(item, ATTR_LAST_REFRESH, 0L));
    // Only publish a metric update once per day
    if (previousRefreshTime.isBefore(today)) {
      final long mediaCount = AttributeValues.getLong(item, ATTR_MEDIA_COUNT, 0L);
      final long bytesUsed = AttributeValues.getLong(item, ATTR_MEDIA_BYTES_USED, 0L);
      final Tags tags = Tags.of(
          UserAgentTagUtil.getPlatformTag(backupUser.userAgent()),
          Tag.of("tier", backupUser.backupLevel().name()));

      DistributionSummary.builder(NUM_OBJECTS_SUMMARY_NAME)
          .tags(tags)
          .publishPercentileHistogram()
          .register(Metrics.globalRegistry)
          .record(mediaCount);
      DistributionSummary.builder(BYTES_USED_SUMMARY_NAME)
          .tags(tags)
          .publishPercentileHistogram()
          .register(Metrics.globalRegistry)
          .record(bytesUsed);

      // Report that the backup is out of quota if it cannot store a max size media object
      final boolean quotaExhausted = bytesUsed >=
          (BackupManager.MAX_TOTAL_BACKUP_MEDIA_BYTES - BackupManager.MAX_MEDIA_OBJECT_SIZE);

      Metrics.counter(BACKUPS_COUNTER_NAME,
              tags.and("quotaExhausted", String.valueOf(quotaExhausted)))
          .increment();
    }
  }

  /**
   * Indicates that we couldn't schedule a deletion because one was already scheduled. The caller may want to delete the
   * objects directly.
   */
  static class PendingDeletionException extends IOException {}

  /**
   * Attempt to mark a backup as expired and swap in a new empty backupDir for the user.
   * <p>
   * After successful completion, the backupDir for the backup-id will be swapped to a new empty directory on the cdn,
   * and the row will be immediately marked eligible for expiration via {@link #getExpiredBackups}.
   * <p>
   * If there is already a pending deletion, this will not swap the backupDir. The expiration timestamps will be
   * updated, but the existing backupDir will remain. The caller should handle this case and start the deletion
   * immediately by catching {@link PendingDeletionException}.
   *
   * @param backupUser The backupUser whose data should be eventually deleted
   * @return A future that completes successfully if the user's data is now inaccessible, or with a
   * {@link PendingDeletionException} if the backupDir could not be changed.
   */
  CompletableFuture<Void> scheduleBackupDeletion(final AuthenticatedBackupUser backupUser) {
    final byte[] hashedBackupId = hashedBackupId(backupUser);

    // Clear usage metadata, swap names of things we intend to delete, and record our intent to delete in attr_expired_prefix
    return dynamoClient.updateItem(new UpdateBuilder(backupTableName, BackupLevel.PAID, hashedBackupId)
            .clearMediaUsage(clock)
            .expireDirectoryNames(secureRandom, ExpiredBackup.ExpirationType.ALL)
            .setRefreshTimes(Instant.ofEpochSecond(0))
            .addSetExpression("#expiredPrefix = :expiredPrefix",
                Map.entry("#expiredPrefix", ATTR_EXPIRED_PREFIX),
                Map.entry(":expiredPrefix", AttributeValues.s(backupUser.backupDir())))
            .withConditionExpression("attribute_not_exists(#expiredPrefix) OR #expiredPrefix = :expiredPrefix")
            .updateItemBuilder()
            .build())
        .exceptionallyCompose(ExceptionUtils.exceptionallyHandler(ConditionalCheckFailedException.class, e ->
            // We already have a pending deletion for this backup-id. This is most likely to occur when the caller
            // is toggling backups on and off. In this case, it should be pretty cheap to directly delete the backup.
            // Instead of changing the backupDir, just make sure the row has expired/ timestamps and tell the caller we
            // couldn't schedule the deletion.
            dynamoClient.updateItem(new UpdateBuilder(backupTableName, BackupLevel.PAID, hashedBackupId)
                    .setRefreshTimes(Instant.ofEpochSecond(0))
                    .updateItemBuilder()
                    .build())
                .thenApply(ignore -> {
                  throw ExceptionUtils.wrap(new PendingDeletionException());
                })))
        .thenRun(Util.NOOP);
  }

  record BackupDescription(int cdn, Optional<Long> mediaUsedSpace) {}

  /**
   * Retrieve information about the backup
   *
   * @param backupUser an already authorized backup user
   * @return A {@link BackupDescription} containing the cdn of the message backup and the total number of media space
   * bytes used by the backup user.
   */
  CompletableFuture<BackupDescription> describeBackup(final AuthenticatedBackupUser backupUser) {
    return dynamoClient.getItem(GetItemRequest.builder()
            .tableName(backupTableName)
            .key(Map.of(KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId(backupUser))))
            .projectionExpression("#cdn,#mediaBytesUsed")
            .expressionAttributeNames(Map.of("#cdn", ATTR_CDN, "#mediaBytesUsed", ATTR_MEDIA_BYTES_USED))
            .consistentRead(true)
            .build())
        .thenApply(response -> {
          if (!response.hasItem()) {
            throw Status.NOT_FOUND.withDescription("Backup ID not found").asRuntimeException();
          }
          // If the client hasn't already uploaded a backup, return the cdn we would return if they did create one
          final int cdn = AttributeValues.getInt(response.item(), ATTR_CDN, BACKUP_CDN);
          final Optional<Long> mediaUsed = AttributeValues.get(response.item(), ATTR_MEDIA_BYTES_USED)
              .map(AttributeValue::n)
              .map(Long::parseLong);

          return new BackupDescription(cdn, mediaUsed);
        });
  }

  public record TimestampedUsageInfo(UsageInfo usageInfo, Instant lastRecalculationTime) {}

  CompletableFuture<TimestampedUsageInfo> getMediaUsage(final AuthenticatedBackupUser backupUser) {
    return dynamoClient.getItem(GetItemRequest.builder()
            .tableName(backupTableName)
            .key(Map.of(KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId(backupUser))))
            .projectionExpression("#mediaBytesUsed,#mediaCount,#usageRecalc")
            .expressionAttributeNames(Map.of(
                "#mediaBytesUsed", ATTR_MEDIA_BYTES_USED,
                "#mediaCount", ATTR_MEDIA_COUNT,
                "#usageRecalc", ATTR_MEDIA_USAGE_LAST_RECALCULATION))
            .consistentRead(true)
            .build())
        .thenApply(response -> {
          final long mediaUsed = AttributeValues.getLong(response.item(), ATTR_MEDIA_BYTES_USED, 0L);
          final long mediaCount = AttributeValues.getLong(response.item(), ATTR_MEDIA_COUNT, 0L);
          final long recalcSeconds = AttributeValues.getLong(response.item(), ATTR_MEDIA_USAGE_LAST_RECALCULATION, 0L);
          return new TimestampedUsageInfo(new UsageInfo(mediaUsed, mediaCount), Instant.ofEpochSecond(recalcSeconds));
        });


  }

  CompletableFuture<Void> setMediaUsage(final AuthenticatedBackupUser backupUser, UsageInfo usageInfo) {
    return setMediaUsage(UpdateBuilder.forUser(backupTableName, backupUser), usageInfo);
  }

  CompletableFuture<Void> setMediaUsage(final StoredBackupAttributes backupAttributes, UsageInfo usageInfo) {
    return setMediaUsage(new UpdateBuilder(backupTableName, BackupLevel.PAID, backupAttributes.hashedBackupId()), usageInfo);
  }

  private CompletableFuture<Void> setMediaUsage(final UpdateBuilder updateBuilder, UsageInfo usageInfo) {
    return dynamoClient.updateItem(
            updateBuilder
                .addSetExpression("#mediaBytesUsed = :mediaBytesUsed",
                    Map.entry("#mediaBytesUsed", ATTR_MEDIA_BYTES_USED),
                    Map.entry(":mediaBytesUsed", AttributeValues.n(usageInfo.bytesUsed())))
                .addSetExpression("#mediaCount = :mediaCount",
                    Map.entry("#mediaCount", ATTR_MEDIA_COUNT),
                    Map.entry(":mediaCount", AttributeValues.n(usageInfo.numObjects())))
                .addSetExpression("#mediaRecalc = :mediaRecalc",
                    Map.entry("#mediaRecalc", ATTR_MEDIA_USAGE_LAST_RECALCULATION),
                    Map.entry(":mediaRecalc", AttributeValues.n(clock.instant().getEpochSecond())))
                .updateItemBuilder()
                .build())
        .thenRun(Util.NOOP);
  }


  /**
   * Marks the backup as undergoing expiration.
   * <p>
   * This must be called before beginning to delete items in the CDN with the prefix specified by
   * {@link ExpiredBackup#prefixToDelete()}. If the prefix has been successfully deleted, {@link #finishExpiration} must
   * be called.
   *
   * @param expiredBackup The backup to expire
   * @return A stage that completes when the backup has been marked for expiration
   */
  CompletableFuture<Void> startExpiration(final ExpiredBackup expiredBackup) {
    if (expiredBackup.expirationType() == ExpiredBackup.ExpirationType.GARBAGE_COLLECTION) {
      // We've already updated the row on a prior (failed) attempt, just need to remove the data from the cdn now
      return CompletableFuture.completedFuture(null);
    }

    // Clear usage metadata, swap names of things we intend to delete, and record our intent to delete in attr_expired_prefix
    return dynamoClient.updateItem(new UpdateBuilder(backupTableName, BackupLevel.PAID, expiredBackup.hashedBackupId())
            .clearMediaUsage(clock)
            .expireDirectoryNames(secureRandom, expiredBackup.expirationType())
            .addRemoveExpression(Map.entry("#mediaRefresh", ATTR_LAST_MEDIA_REFRESH))
            .addSetExpression("#expiredPrefix = :expiredPrefix",
                Map.entry("#expiredPrefix", ATTR_EXPIRED_PREFIX),
                Map.entry(":expiredPrefix", AttributeValues.s(expiredBackup.prefixToDelete())))
            .withConditionExpression("attribute_not_exists(#expiredPrefix) OR #expiredPrefix = :expiredPrefix")
            .updateItemBuilder()
            .build())
        .thenRun(Util.NOOP);
  }

  /**
   * Complete expiration of a backup started with {@link #startExpiration}
   * <p>
   * If the expiration was for the entire backup, this will delete the entire item for the backup.
   *
   * @param expiredBackup The backup to expire
   * @return A stage that completes when the expiration is marked as finished
   */
  CompletableFuture<Void> finishExpiration(final ExpiredBackup expiredBackup) {
    final byte[] hashedBackupId = expiredBackup.hashedBackupId();
    if (expiredBackup.expirationType() == ExpiredBackup.ExpirationType.ALL) {
      final long expectedLastRefresh = expiredBackup.lastRefresh().getEpochSecond();
      return dynamoClient.deleteItem(DeleteItemRequest.builder()
              .tableName(backupTableName)
              .key(Map.of(KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId)))
              .conditionExpression("#lastRefresh <= :expectedLastRefresh")
              .expressionAttributeNames(Map.of("#lastRefresh", ATTR_LAST_REFRESH))
              .expressionAttributeValues(Map.of(":expectedLastRefresh", AttributeValues.n(expectedLastRefresh)))
              .build())
          .thenRun(Util.NOOP);
    } else {
      return dynamoClient.updateItem(new UpdateBuilder(backupTableName, BackupLevel.PAID, hashedBackupId)
              .addRemoveExpression(Map.entry("#expiredPrefixes", ATTR_EXPIRED_PREFIX))
              .updateItemBuilder()
              .build())
          .thenRun(Util.NOOP);
    }
  }

  Flux<StoredBackupAttributes> listBackupAttributes(final int segments, final Scheduler scheduler) {
    if (segments < 1) {
      throw new IllegalArgumentException("Total number of segments must be positive");
    }

    return Flux.range(0, segments)
        .parallel()
        .runOn(scheduler)
        .flatMap(segment -> dynamoClient.scanPaginator(ScanRequest.builder()
                .tableName(backupTableName)
                .consistentRead(true)
                .segment(segment)
                .totalSegments(segments)
                .expressionAttributeNames(Map.of(
                    "#backupIdHash", KEY_BACKUP_ID_HASH,
                    "#refresh", ATTR_LAST_REFRESH,
                    "#mediaRefresh", ATTR_LAST_MEDIA_REFRESH,
                    "#bytesUsed", ATTR_MEDIA_BYTES_USED,
                    "#numObjects", ATTR_MEDIA_COUNT,
                    "#backupDir", ATTR_BACKUP_DIR,
                    "#mediaDir", ATTR_MEDIA_DIR))
                .projectionExpression("#backupIdHash, #refresh, #mediaRefresh, #bytesUsed, #numObjects, #backupDir, #mediaDir")
                .build())
            .items())
        .sequential()
        .filter(item -> item.containsKey(KEY_BACKUP_ID_HASH))
        .map(item -> new StoredBackupAttributes(
            AttributeValues.getByteArray(item, KEY_BACKUP_ID_HASH, null),
            AttributeValues.getString(item, ATTR_BACKUP_DIR, null),
            AttributeValues.getString(item, ATTR_MEDIA_DIR, null),
            Instant.ofEpochSecond(AttributeValues.getLong(item, ATTR_LAST_REFRESH, 0L)),
            Instant.ofEpochSecond(AttributeValues.getLong(item, ATTR_LAST_MEDIA_REFRESH, 0L)),
            AttributeValues.getLong(item, ATTR_MEDIA_BYTES_USED, 0L),
            AttributeValues.getLong(item, ATTR_MEDIA_COUNT, 0L)));
  }

  Flux<ExpiredBackup> getExpiredBackups(final int segments, final Scheduler scheduler, final Instant purgeTime) {
    if (segments < 1) {
      throw new IllegalArgumentException("Total number of segments must be positive");
    }

    return Flux.range(0, segments)
        .parallel()
        .runOn(scheduler)
        .flatMap(segment -> dynamoClient.scanPaginator(ScanRequest.builder()
                .tableName(backupTableName)
                .consistentRead(true)
                .segment(segment)
                .totalSegments(segments)
                .expressionAttributeNames(Map.of(
                    "#backupIdHash", KEY_BACKUP_ID_HASH,
                    "#refresh", ATTR_LAST_REFRESH,
                    "#mediaRefresh", ATTR_LAST_MEDIA_REFRESH,
                    "#backupDir", ATTR_BACKUP_DIR,
                    "#mediaDir", ATTR_MEDIA_DIR,
                    "#expiredPrefix", ATTR_EXPIRED_PREFIX))
                .expressionAttributeValues(Map.of(":purgeTime", AttributeValues.n(purgeTime.getEpochSecond())))
                .projectionExpression("#backupIdHash, #refresh, #mediaRefresh, #backupDir, #mediaDir, #expiredPrefix")
                .filterExpression(
                    "(#refresh < :purgeTime) OR (#mediaRefresh < :purgeTime) OR attribute_exists(#expiredPrefix)")
                .build())
            .items())
        .sequential()
        .filter(Predicate.not(Map::isEmpty))
        .mapNotNull(item -> {
          final byte[] hashedBackupId = AttributeValues.getByteArray(item, KEY_BACKUP_ID_HASH, null);
          if (hashedBackupId == null) {
            return null;
          }
          final String backupDir = AttributeValues.getString(item, ATTR_BACKUP_DIR, null);
          final String mediaDir = AttributeValues.getString(item, ATTR_MEDIA_DIR, null);
          if (backupDir == null || mediaDir == null) {
            // Could be the case for backups that have not yet set a public key
            return null;
          }
          final long lastRefresh = AttributeValues.getLong(item, ATTR_LAST_REFRESH, Long.MAX_VALUE);
          final long lastMediaRefresh = AttributeValues.getLong(item, ATTR_LAST_MEDIA_REFRESH, Long.MAX_VALUE);
          final String existingExpiration = AttributeValues.getString(item, ATTR_EXPIRED_PREFIX, null);

          final ExpiredBackup expiredBackup;
          if (existingExpiration != null) {
            // If we have work from a failed previous expiration, handle that before worrying about any new expirations.
            // This guarantees we won't accumulate expirations
            expiredBackup = new ExpiredBackup(hashedBackupId, ExpiredBackup.ExpirationType.GARBAGE_COLLECTION,
                Instant.ofEpochSecond(lastRefresh), existingExpiration);
          } else if (lastRefresh < purgeTime.getEpochSecond()) {
            // The whole backup was expired
            expiredBackup = new ExpiredBackup(hashedBackupId, ExpiredBackup.ExpirationType.ALL,
                Instant.ofEpochSecond(lastRefresh), backupDir);
          } else if (lastMediaRefresh < purgeTime.getEpochSecond()) {
            // The media was expired
            expiredBackup = new ExpiredBackup(hashedBackupId, ExpiredBackup.ExpirationType.MEDIA,
                Instant.ofEpochSecond(lastRefresh), backupDir + "/" + mediaDir);
          } else {
            return null;
          }

          if (!isValid(expiredBackup)) {
            logger.error("Not expiring backup {} for backupId {} with invalid cdn path prefixes",
                HexFormat.of().formatHex(expiredBackup.hashedBackupId()),
                expiredBackup);
            return null;
          }
          return expiredBackup;
        });
  }

  /**
   * Backup expiration will expire any prefix we tell it to, so confirm that the directory names that came out of the
   * database have the correct shape before handing them off.
   *
   * @param expiredBackup The ExpiredBackup object to check
   * @return Whether this is a valid expiration object
   */
  private static boolean isValid(final ExpiredBackup expiredBackup) {
    // expired prefixes should be of the form "backupDir" or "backupDir/mediaDir"
    return switch (expiredBackup.expirationType()) {
      case MEDIA -> expiredBackup.prefixToDelete().length() == MEDIA_DIRECTORY_PATH_LENGTH;
      case ALL -> expiredBackup.prefixToDelete().length() == BACKUP_DIRECTORY_PATH_LENGTH;
      case GARBAGE_COLLECTION -> expiredBackup.prefixToDelete().length() == MEDIA_DIRECTORY_PATH_LENGTH ||
          expiredBackup.prefixToDelete().length() == BACKUP_DIRECTORY_PATH_LENGTH;
    };
  }

  /**
   * Build ddb update statements for the backups table
   */
  private static class UpdateBuilder {

    private final List<String> setStatements = new ArrayList<>();
    private final List<String> removeStatements = new ArrayList<>();
    private final Map<String, AttributeValue> attrValues = new HashMap<>();
    private final Map<String, String> attrNames = new HashMap<>();

    private final String tableName;
    private final BackupLevel backupLevel;
    private final byte[] hashedBackupId;
    private String conditionExpression = null;

    static UpdateBuilder forUser(String tableName, AuthenticatedBackupUser backupUser) {
      return new UpdateBuilder(tableName, backupUser.backupLevel(), hashedBackupId(backupUser));
    }

    UpdateBuilder(String tableName, BackupLevel backupLevel, byte[] hashedBackupId) {
      this.tableName = tableName;
      this.backupLevel = backupLevel;
      this.hashedBackupId = hashedBackupId;
    }

    private void addAttrValue(Map.Entry<String, AttributeValue> attrValue) {
      final AttributeValue old = attrValues.put(attrValue.getKey(), attrValue.getValue());
      if (old != null && !old.equals(attrValue.getValue())) {
        throw new IllegalArgumentException("duplicate attrValue key used for different values");
      }
    }

    private void addAttrName(Map.Entry<String, String> attrName) {
      final String oldName = attrNames.put(attrName.getKey(), attrName.getValue());
      if (oldName != null && !oldName.equals(attrName.getValue())) {
        throw new IllegalArgumentException("duplicate attrName key used for different attribute names");
      }
    }

    private void addAttrs(final Map.Entry<String, String> attrName, final Map.Entry<String, AttributeValue> attrValue) {
      addAttrName(attrName);
      addAttrValue(attrValue);
    }

    UpdateBuilder addSetExpression(
        final String update,
        final Map.Entry<String, String> attrName,
        final Map.Entry<String, AttributeValue> attrValue) {
      setStatements.add(update);
      addAttrs(attrName, attrValue);
      return this;
    }

    UpdateBuilder addSetExpression(final String update) {
      setStatements.add(update);
      return this;
    }

    UpdateBuilder addRemoveExpression(final Map.Entry<String, String> attrName) {
      addAttrName(attrName);
      removeStatements.add(attrName.getKey());
      return this;
    }

    UpdateBuilder withConditionExpression(final String conditionExpression) {
      this.conditionExpression = conditionExpression;
      return this;
    }

    UpdateBuilder withConditionExpression(
        final String conditionExpression,
        final Map.Entry<String, String> attrName,
        final Map.Entry<String, AttributeValue> attrValue) {
      this.addAttrs(attrName, attrValue);
      this.conditionExpression = conditionExpression;
      return this;
    }

    UpdateBuilder setCdn(final int cdn) {
      return addSetExpression(
          "#cdn = :cdn",
          Map.entry("#cdn", ATTR_CDN),
          Map.entry(":cdn", AttributeValues.n(cdn)));
    }

    UpdateBuilder incrementMediaCount(long delta) {
      addAttrName(Map.entry("#mediaCount", ATTR_MEDIA_COUNT));
      addAttrValue(Map.entry(":zero", AttributeValues.n(0)));
      addAttrValue(Map.entry(":mediaCountDelta", AttributeValues.n(delta)));
      addSetExpression("#mediaCount = if_not_exists(#mediaCount, :zero) + :mediaCountDelta");
      return this;
    }

    UpdateBuilder incrementMediaBytes(long delta) {
      addAttrName(Map.entry("#mediaBytes", ATTR_MEDIA_BYTES_USED));
      addAttrValue(Map.entry(":zero", AttributeValues.n(0)));
      addAttrValue(Map.entry(":mediaBytesDelta", AttributeValues.n(delta)));
      addSetExpression("#mediaBytes = if_not_exists(#mediaBytes, :zero) + :mediaBytesDelta");
      return this;
    }

    UpdateBuilder clearMediaUsage(final Clock clock) {
      addSetExpression("#mediaBytesUsed = :mediaBytesUsed",
          Map.entry("#mediaBytesUsed", ATTR_MEDIA_BYTES_USED),
          Map.entry(":mediaBytesUsed", AttributeValues.n(0L)));
      addSetExpression("#mediaCount = :mediaCount",
          Map.entry("#mediaCount", ATTR_MEDIA_COUNT),
          Map.entry(":mediaCount", AttributeValues.n(0L)));
      addSetExpression("#mediaRecalc = :mediaRecalc",
          Map.entry("#mediaRecalc", ATTR_MEDIA_USAGE_LAST_RECALCULATION),
          Map.entry(":mediaRecalc", AttributeValues.n(clock.instant().getEpochSecond())));
      return this;
    }

    UpdateBuilder setDirectoryNamesIfMissing(final SecureRandom secureRandom) {
      final String backupDir = generateDirName(secureRandom);
      final String mediaDir = generateDirName(secureRandom);
      addSetExpression("#backupDir = if_not_exists(#backupDir, :backupDir)",
          Map.entry("#backupDir", ATTR_BACKUP_DIR),
          Map.entry(":backupDir", AttributeValues.s(backupDir)));

      addSetExpression("#mediaDir = if_not_exists(#mediaDir, :mediaDir)",
          Map.entry("#mediaDir", ATTR_MEDIA_DIR),
          Map.entry(":mediaDir", AttributeValues.s(mediaDir)));
      return this;
    }

    UpdateBuilder expireDirectoryNames(
        final SecureRandom secureRandom,
        final ExpiredBackup.ExpirationType expirationType) {
      final String backupDir = generateDirName(secureRandom);
      final String mediaDir = generateDirName(secureRandom);
      return switch (expirationType) {
        case GARBAGE_COLLECTION -> this;
        case MEDIA -> this.addSetExpression("#mediaDir = :mediaDir",
            Map.entry("#mediaDir", ATTR_MEDIA_DIR),
            Map.entry(":mediaDir", AttributeValues.s(mediaDir)));
        case ALL -> this
            .addSetExpression("#mediaDir = :mediaDir",
                Map.entry("#mediaDir", ATTR_MEDIA_DIR),
                Map.entry(":mediaDir", AttributeValues.s(mediaDir)))
            .addSetExpression("#backupDir = :backupDir",
                Map.entry("#backupDir", ATTR_BACKUP_DIR),
                Map.entry(":backupDir", AttributeValues.s(backupDir)));
      };
    }

    UpdateBuilder setRefreshTimes(final Clock clock) {
      return setRefreshTimes(clock.instant().truncatedTo(ChronoUnit.DAYS));
    }

    /**
     * Set the lastRefresh time as part of the update
     * <p>
     * This always updates lastRefreshTime, and updates lastMediaRefreshTime if the backup user has the appropriate
     * level.
     */
    UpdateBuilder setRefreshTimes(final Instant refreshTime) {
      if (!refreshTime.truncatedTo(ChronoUnit.DAYS).equals(refreshTime)) {
        throw new IllegalArgumentException("Refresh time must be day aligned");
      }
      addSetExpression("#lastRefreshTime = :lastRefreshTime",
          Map.entry("#lastRefreshTime", ATTR_LAST_REFRESH),
          Map.entry(":lastRefreshTime", AttributeValues.n(refreshTime.getEpochSecond())));

      if (backupLevel.compareTo(BackupLevel.PAID) >= 0) {
        // update the media time if we have the appropriate level
        addSetExpression("#lastMediaRefreshTime = :lastMediaRefreshTime",
            Map.entry("#lastMediaRefreshTime", ATTR_LAST_MEDIA_REFRESH),
            Map.entry(":lastMediaRefreshTime", AttributeValues.n(refreshTime.getEpochSecond())));
      }
      return this;
    }

    private String updateExpression() {
      final StringBuilder sb = new StringBuilder();
      if (!setStatements.isEmpty()) {
        sb.append("SET ");
        sb.append(String.join(",", setStatements));
      }
      if (!removeStatements.isEmpty()) {
        sb.append(" REMOVE ");
        sb.append(String.join(",", removeStatements));
      }
      return sb.toString();
    }

    /**
     * Prepare a non-transactional update
     *
     * @return An {@link UpdateItemRequest#builder()} that can be used with updateItem
     */
    UpdateItemRequest.Builder updateItemBuilder() {
      final UpdateItemRequest.Builder bldr = UpdateItemRequest.builder()
          .tableName(tableName)
          .key(Map.of(KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId)))
          .updateExpression(updateExpression())
          .expressionAttributeNames(attrNames);
      if (!this.attrValues.isEmpty()) {
        bldr.expressionAttributeValues(attrValues);
      }
      if (this.conditionExpression != null) {
        bldr.conditionExpression(conditionExpression);
      }
      return bldr;
    }

    /**
     * Prepare a transactional update
     *
     * @return An {@link Update#builder()} that can be used with transactItem
     */
    Update.Builder transactItemBuilder() {
      final Update.Builder bldr = Update.builder()
          .tableName(tableName)
          .key(Map.of(KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId)))
          .updateExpression(updateExpression())
          .expressionAttributeNames(attrNames)
          .expressionAttributeValues(attrValues);
      if (this.conditionExpression != null) {
        bldr.conditionExpression(conditionExpression);
      }
      return bldr;
    }
  }

  static String generateDirName(final SecureRandom secureRandom) {
    final byte[] bytes = new byte[16];
    secureRandom.nextBytes(bytes);
    return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
  }

  private static byte[] hashedBackupId(final AuthenticatedBackupUser backupId) {
    return hashedBackupId(backupId.backupId());
  }

  static byte[] hashedBackupId(final byte[] backupId) {
    try {
      return Arrays.copyOf(MessageDigest.getInstance("SHA-256").digest(backupId), 16);
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }
}
