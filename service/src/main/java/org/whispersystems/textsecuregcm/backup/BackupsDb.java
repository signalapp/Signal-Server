package org.whispersystems.textsecuregcm.backup;

import io.grpc.Status;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.Util;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Update;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * Tracks backup metadata in a persistent store.
 * <p>
 * It's assumed that the caller has already validated that the backupUser being operated on has valid credentials and
 * possesses the appropriate {@link BackupTier} to perform the current operation.
 */
public class BackupsDb {

  private static final Logger logger = LoggerFactory.getLogger(BackupsDb.class);
  static final int BACKUP_CDN = 3;

  private final DynamoDbAsyncClient dynamoClient;
  private final String backupTableName;
  private final Clock clock;

  // The backups table

  // B: 16 bytes that identifies the backup
  public static final String KEY_BACKUP_ID_HASH = "U";
  // N: Time in seconds since epoch of the last backup refresh. This timestamp must be periodically updated to avoid
  // garbage collection of archive objects.
  public static final String ATTR_LAST_REFRESH = "R";
  // N: Time in seconds since epoch of the last backup media refresh. This timestamp can only be updated if the client
  // has BackupTier.MEDIA, and must be periodically updated to avoid garbage collection of media objects.
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

  public BackupsDb(
      final DynamoDbAsyncClient dynamoClient,
      final String backupTableName,
      final Clock clock) {
    this.dynamoClient = dynamoClient;
    this.backupTableName = backupTableName;
    this.clock = clock;
  }

  /**
   * Set the public key associated with a backupId.
   *
   * @param authenticatedBackupId   The backup-id bytes that should be associated with the provided public key
   * @param authenticatedBackupTier The backup tier
   * @param publicKey               The public key to associate with the backup id
   * @return A stage that completes when the public key has been set. If the backup-id already has a set public key that
   * does not match, the stage will be completed exceptionally with a {@link PublicKeyConflictException}
   */
  CompletableFuture<Void> setPublicKey(
      final byte[] authenticatedBackupId,
      final BackupTier authenticatedBackupTier,
      final ECPublicKey publicKey) {
    final byte[] hashedBackupId = hashedBackupId(authenticatedBackupId);
    return dynamoClient.updateItem(new UpdateBuilder(backupTableName, authenticatedBackupTier, hashedBackupId)
            .addSetExpression("#publicKey = :publicKey",
                Map.entry("#publicKey", ATTR_PUBLIC_KEY),
                Map.entry(":publicKey", AttributeValues.b(publicKey.serialize())))
            .setRefreshTimes(clock)
            .withConditionExpression("attribute_not_exists(#publicKey) OR #publicKey = :publicKey")
            .updateItemBuilder()
            .build())
        .exceptionally(throwable -> {
          // There was already a row for this backup-id and it contained a different publicKey
          if (ExceptionUtils.unwrap(throwable) instanceof ConditionalCheckFailedException) {
            throw ExceptionUtils.wrap(new PublicKeyConflictException());
          }
          throw ExceptionUtils.wrap(throwable);
        })
        .thenRun(Util.NOOP);
  }

  CompletableFuture<Optional<byte[]>> retrievePublicKey(byte[] backupId) {
    final byte[] hashedBackupId = hashedBackupId(backupId);
    return dynamoClient.getItem(GetItemRequest.builder()
            .tableName(backupTableName)
            .key(Map.of(KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId)))
            .consistentRead(true)
            .projectionExpression("#publicKey")
            .expressionAttributeNames(Map.of("#publicKey", ATTR_PUBLIC_KEY))
            .build())
        .thenApply(response ->
            AttributeValues.get(response.item(), ATTR_PUBLIC_KEY)
                .map(AttributeValue::b)
                .map(SdkBytes::asByteArray));
  }


  /**
   * Update the quota in the backup table
   *
   * @param backupUser  The backup user
   * @param mediaBytesDelta The length of the media after encryption. A negative length implies media being removed
   * @param mediaCountDelta The number of media objects being added, or if negative, removed
   * @return A stage that completes successfully once the table are updated.
   */
  CompletableFuture<Void> trackMedia(final AuthenticatedBackupUser backupUser, final long mediaCountDelta, final long mediaBytesDelta) {
    final Instant now = clock.instant();
    return dynamoClient
        .updateItem(
            // Update the media quota and TTL
            UpdateBuilder.forUser(backupTableName, backupUser)
                .setRefreshTimes(now)
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
    // update message backup TTL
    return dynamoClient.updateItem(UpdateBuilder.forUser(backupTableName, backupUser)
            .setRefreshTimes(clock)
            .updateItemBuilder()
            .build())
        .thenRun(Util.NOOP);
  }

  /**
   * Track that a backup will be stored for the user
   *
   * @param backupUser an already authorized backup user
   */
  CompletableFuture<Void> addMessageBackup(final AuthenticatedBackupUser backupUser) {
    // this could race with concurrent updates, but the only effect would be last-writer-wins on the timestamp
    return dynamoClient.updateItem(
            UpdateBuilder.forUser(backupTableName, backupUser)
                .setRefreshTimes(clock)
                .setCdn(BACKUP_CDN)
                .updateItemBuilder()
                .build())
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
            throw Status.NOT_FOUND.withDescription("Backup not found").asRuntimeException();
          }
          final int cdn = AttributeValues.get(response.item(), ATTR_CDN)
              .map(AttributeValue::n)
              .map(Integer::parseInt)
              .orElseThrow(() -> Status.NOT_FOUND.withDescription("Stored backup not found").asRuntimeException());

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
    return dynamoClient.updateItem(
            UpdateBuilder.forUser(backupTableName, backupUser)
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
   * Build ddb update statements for the backups table
   */
  private static class UpdateBuilder {

    private final List<String> setStatements = new ArrayList<>();
    private final Map<String, AttributeValue> attrValues = new HashMap<>();
    private final Map<String, String> attrNames = new HashMap<>();

    private final String tableName;
    private final BackupTier backupTier;
    private final byte[] hashedBackupId;
    private String conditionExpression = null;

    static UpdateBuilder forUser(String tableName, AuthenticatedBackupUser backupUser) {
      return new UpdateBuilder(tableName, backupUser.backupTier(), hashedBackupId(backupUser));
    }

    UpdateBuilder(String tableName, BackupTier backupTier, byte[] hashedBackupId) {
      this.tableName = tableName;
      this.backupTier = backupTier;
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

    /**
     * Set the lastRefresh time as part of the update
     * <p>
     * This always updates lastRefreshTime, and updates lastMediaRefreshTime if the backup user has the appropriate
     * tier.
     */
    UpdateBuilder setRefreshTimes(final Clock clock) {
      return this.setRefreshTimes(clock.instant());
    }

    UpdateBuilder setRefreshTimes(final Instant refreshTime) {
      addSetExpression("#lastRefreshTime = :lastRefreshTime",
          Map.entry("#lastRefreshTime", ATTR_LAST_REFRESH),
          Map.entry(":lastRefreshTime", AttributeValues.n(refreshTime.getEpochSecond())));

      if (backupTier.compareTo(BackupTier.MEDIA) >= 0) {
        // update the media time if we have the appropriate tier
        addSetExpression("#lastMediaRefreshTime = :lastMediaRefreshTime",
            Map.entry("#lastMediaRefreshTime", ATTR_LAST_MEDIA_REFRESH),
            Map.entry(":lastMediaRefreshTime", AttributeValues.n(refreshTime.getEpochSecond())));
      }
      return this;
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
          .updateExpression("SET %s".formatted(String.join(",", setStatements)))
          .expressionAttributeNames(attrNames)
          .expressionAttributeValues(attrValues);
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
          .updateExpression("SET %s".formatted(String.join(",", setStatements)))
          .expressionAttributeNames(attrNames)
          .expressionAttributeValues(attrValues);
      if (this.conditionExpression != null) {
        bldr.conditionExpression(conditionExpression);
      }
      return bldr;
    }
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
