package org.whispersystems.textsecuregcm.backup;

import io.grpc.Status;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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
import software.amazon.awssdk.services.dynamodb.model.CancellationReason;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.Delete;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Put;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItemsRequest;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;
import software.amazon.awssdk.services.dynamodb.model.Update;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

/**
 * Tracks backup metadata in a persistent store.
 *
 * It's assumed that the caller has already validated that the backupUser being operated on has valid credentials and
 * possesses the appropriate {@link BackupTier} to perform the current operation.
 */
public class BackupsDb {
  private static final Logger logger = LoggerFactory.getLogger(BackupsDb.class);
  static final int BACKUP_CDN = 3;

  private final DynamoDbAsyncClient dynamoClient;
  private final String backupTableName;
  private final String backupMediaTableName;
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

  // The stored media table (hashedBackupId, mediaId, cdn, objectLength)

  // B: 15-byte mediaId
  public static final String KEY_MEDIA_ID = "M";
  // N: The length of the encrypted media object
  public static final String ATTR_LENGTH = "L";

  public BackupsDb(
      final DynamoDbAsyncClient dynamoClient,
      final String backupTableName,
      final String backupMediaTableName,
      final Clock clock) {
    this.dynamoClient = dynamoClient;
    this.backupTableName = backupTableName;
    this.backupMediaTableName = backupMediaTableName;
    this.clock = clock;
  }

  /**
   * Set the public key associated with a backupId.
   *
   * @param authenticatedBackupId The backup-id bytes that should be associated with the provided public key
   * @param authenticatedBackupTier The backup tier
   * @param publicKey The public key to associate with the backup id
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
   * Add media to the backup media table and update the quota in the backup table
   *
   * @param backupUser  The
   * @param mediaId     The mediaId to add
   * @param mediaLength The length of the media before encryption (the length of the source media)
   * @return A stage that completes successfully once the tables are updated. If the media with the provided id has
   * previously been tracked with a different length, the stage will complete exceptionally with an
   * {@link InvalidLengthException}
   */
  CompletableFuture<Void> trackMedia(
      final AuthenticatedBackupUser backupUser,
      final byte[] mediaId,
      final int mediaLength) {
    final byte[] hashedBackupId = hashedBackupId(backupUser);
    return dynamoClient
        .transactWriteItems(TransactWriteItemsRequest.builder().transactItems(

            // Add the media to the media table
            TransactWriteItem.builder().put(Put.builder()
                .tableName(backupMediaTableName)
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                .item(Map.of(
                    KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId),
                    KEY_MEDIA_ID, AttributeValues.b(mediaId),
                    ATTR_CDN, AttributeValues.n(BACKUP_CDN),
                    ATTR_LENGTH, AttributeValues.n(mediaLength)))
                .conditionExpression("attribute_not_exists(#mediaId)")
                .expressionAttributeNames(Map.of("#mediaId", KEY_MEDIA_ID))
                .build()).build(),

            // Update the media quota and TTL
            TransactWriteItem.builder().update(
                UpdateBuilder.forUser(backupTableName, backupUser)
                    .setRefreshTimes(clock)
                    .incrementMediaBytes(mediaLength)
                    .incrementMediaCount(1)
                    .transactItemBuilder()
                    .build()).build()).build())
        .exceptionally(throwable -> {
          if (ExceptionUtils.unwrap(throwable) instanceof TransactionCanceledException txCancelled) {
            final long oldItemLength = conditionCheckFailed(txCancelled, 0)
                .flatMap(item -> Optional.ofNullable(item.get(ATTR_LENGTH)))
                .map(attr -> Long.parseLong(attr.n()))
                .orElseThrow(() -> ExceptionUtils.wrap(throwable));
            if (oldItemLength != mediaLength) {
              throw new CompletionException(
                  new InvalidLengthException("Previously tried to copy media with a different length. "
                      + "Provided " + mediaLength + " was " + oldItemLength));
            }
            // The client already "paid" for this media, can let them through
            return null;
          } else {
            // rethrow original exception
            throw ExceptionUtils.wrap(throwable);
          }
        })
        .thenRun(Util.NOOP);
  }

  /**
   * Remove media from backup media table and update the quota in the backup table
   *
   * @param backupUser  The backup user
   * @param mediaId     The mediaId to add
   * @param mediaLength The length of the media before encryption (the length of the source media)
   * @return A stage that completes successfully once the tables are updated
   */
  CompletableFuture<Void> untrackMedia(
      final AuthenticatedBackupUser backupUser,
      final byte[] mediaId,
      final int mediaLength) {
    final byte[] hashedBackupId = hashedBackupId(backupUser);
    return dynamoClient.transactWriteItems(TransactWriteItemsRequest.builder().transactItems(
            TransactWriteItem.builder().delete(Delete.builder()
                .tableName(backupMediaTableName)
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                .key(Map.of(
                    KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId),
                    KEY_MEDIA_ID, AttributeValues.b(mediaId)
                ))
                .conditionExpression("#length = :length")
                .expressionAttributeNames(Map.of("#length", ATTR_LENGTH))
                .expressionAttributeValues(Map.of(":length", AttributeValues.n(mediaLength)))
                .build()).build(),

            // Don't update TTLs, since we're just cleaning up media
            TransactWriteItem.builder().update(UpdateBuilder.forUser(backupTableName, backupUser)
                .incrementMediaBytes(-mediaLength)
                .incrementMediaCount(-1)
                .transactItemBuilder().build()).build()).build())
        .exceptionally(error -> {
          logger.warn("failed cleanup after failed copy operation", error);
          return null;
        })
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
            .projectionExpression("#cdn,#bytesUsed")
            .expressionAttributeNames(Map.of("#cdn", ATTR_CDN, "#bytesUsed", ATTR_MEDIA_BYTES_USED))
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
     * tier
     */
    UpdateBuilder setRefreshTimes(final Clock clock) {
      final long refreshTimeSecs = clock.instant().getEpochSecond();
      addSetExpression("#lastRefreshTime = :lastRefreshTime",
          Map.entry("#lastRefreshTime", ATTR_LAST_REFRESH),
          Map.entry(":lastRefreshTime", AttributeValues.n(refreshTimeSecs)));

      if (backupTier.compareTo(BackupTier.MEDIA) >= 0) {
        // update the media time if we have the appropriate tier
        addSetExpression("#lastMediaRefreshTime = :lastMediaRefreshTime",
            Map.entry("#lastMediaRefreshTime", ATTR_LAST_MEDIA_REFRESH),
            Map.entry(":lastMediaRefreshTime", AttributeValues.n(refreshTimeSecs)));
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

  /**
   * Check if a DynamoDb error indicates a condition check failed error, and return the value of the item failed to
   * update.
   *
   * @param e         The error returned by {@link DynamoDbAsyncClient#transactWriteItems} attempt
   * @param itemIndex The index of the item in the transaction that had a condition expression
   * @return The remote value of the item that failed to update, or empty if the error was not a condition check failure
   */
  private static Optional<Map<String, AttributeValue>> conditionCheckFailed(TransactionCanceledException e,
      int itemIndex) {
    if (!e.hasCancellationReasons()) {
      return Optional.empty();
    }
    if (e.cancellationReasons().size() < itemIndex + 1) {
      return Optional.empty();
    }
    final CancellationReason reason = e.cancellationReasons().get(itemIndex);
    if (!"ConditionalCheckFailed".equals(reason.code()) || !reason.hasItem()) {
      return Optional.empty();
    }
    return Optional.of(reason.item());
  }

}
