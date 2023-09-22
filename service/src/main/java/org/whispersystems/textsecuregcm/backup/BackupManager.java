/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import io.grpc.Status;
import io.micrometer.core.instrument.Metrics;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.Util;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

public class BackupManager {

  private static final Logger logger = LoggerFactory.getLogger(BackupManager.class);

  static final String MESSAGE_BACKUP_NAME = "messageBackup";
  private static final int BACKUP_CDN = 3;
  private static final String ZK_AUTHN_COUNTER_NAME = MetricsUtil.name(BackupManager.class, "authentication");
  private static final String ZK_AUTHZ_FAILURE_COUNTER_NAME = MetricsUtil.name(BackupManager.class, "authorizationFailure");
  private static final String SUCCESS_TAG_NAME = "success";
  private static final String FAILURE_REASON_TAG_NAME = "reason";

  private final GenericServerSecretParams serverSecretParams;
  private final TusBackupCredentialGenerator tusBackupCredentialGenerator;
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

  public BackupManager(
      final GenericServerSecretParams serverSecretParams,
      final TusBackupCredentialGenerator tusBackupCredentialGenerator,
      final DynamoDbAsyncClient dynamoClient,
      final String backupTableName,
      final Clock clock) {
    this.serverSecretParams = serverSecretParams;
    this.dynamoClient = dynamoClient;
    this.tusBackupCredentialGenerator = tusBackupCredentialGenerator;
    this.backupTableName = backupTableName;
    this.clock = clock;
  }

  /**
   * Set the public key for the backup-id.
   * <p>
   * Once set, calls {@link BackupManager#authenticateBackupUser} can succeed if the presentation is signed with the
   * private key corresponding to this public key.
   *
   * @param presentation a ZK credential presentation that encodes the backupId
   * @param signature    the signature of the presentation
   * @param publicKey    the public key of a key-pair that the presentation must be signed with
   */
  public CompletableFuture<Void> setPublicKey(
      final BackupAuthCredentialPresentation presentation,
      final byte[] signature,
      final ECPublicKey publicKey) {

    // Note: this is a special case where we can't validate the presentation signature against the stored public key
    // because we are currently setting it. We check against the provided public key, but we must also verify that
    // there isn't an existing, different stored public key for the backup-id (verified with a condition expression)
    final BackupTier backupTier = verifySignatureAndCheckPresentation(presentation, signature, publicKey);
    if (backupTier.compareTo(BackupTier.MESSAGES) < 0) {
      Metrics.counter(ZK_AUTHZ_FAILURE_COUNTER_NAME).increment();
      throw Status.PERMISSION_DENIED
          .withDescription("credential does not support setting public key")
          .asRuntimeException();
    }

    final byte[] hashedBackupId = hashedBackupId(presentation.getBackupId());
    return dynamoClient.updateItem(UpdateItemRequest.builder()
            .tableName(backupTableName)
            .key(Map.of(KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId)))
            .updateExpression("SET #publicKey = :publicKey")
            .expressionAttributeNames(Map.of("#publicKey", ATTR_PUBLIC_KEY))
            .expressionAttributeValues(Map.of(":publicKey", AttributeValues.b(publicKey.serialize())))
            .conditionExpression("attribute_not_exists(#publicKey) OR #publicKey = :publicKey")
            .build())
        .exceptionally(throwable -> {
          // There was already a row for this backup-id and it contained a different publicKey
          if (ExceptionUtils.unwrap(throwable) instanceof ConditionalCheckFailedException) {
            Metrics.counter(ZK_AUTHN_COUNTER_NAME,
                    SUCCESS_TAG_NAME, String.valueOf(false),
                    FAILURE_REASON_TAG_NAME, "public_key_conflict")
                .increment();
            throw Status.UNAUTHENTICATED
                .withDescription("public key does not match existing public key for the backup-id")
                .asRuntimeException();
          }
          throw ExceptionUtils.wrap(throwable);
        })
        .thenRun(Util.NOOP);
  }


  /**
   * Create a form that may be used to upload a backup file for the backupId encoded in the presentation.
   * <p>
   * If successful, this also updates the TTL of the backup.
   *
   * @param backupUser an already ZK authenticated backup user
   * @return the upload form
   */
  public CompletableFuture<MessageBackupUploadDescriptor> createMessageBackupUploadDescriptor(
      final AuthenticatedBackupUser backupUser) {
    final byte[] hashedBackupId = hashedBackupId(backupUser);
    final String encodedBackupId = encodeForCdn(hashedBackupId);

    final long refreshTimeSecs = clock.instant().getEpochSecond();

    final List<String> updates = new ArrayList<>(List.of("#cdn = :cdn", "#lastRefresh = :expiration"));
    final Map<String, String> expressionAttributeNames = new HashMap<>(Map.of(
        "#cdn", ATTR_CDN,
        "#lastRefresh", ATTR_LAST_REFRESH));
    if (backupUser.backupTier().compareTo(BackupTier.MEDIA) >= 0) {
      updates.add("#lastMediaRefresh = :expiration");
      expressionAttributeNames.put("#lastMediaRefresh", ATTR_LAST_MEDIA_REFRESH);
    }

    // this could race with concurrent updates, but the only effect would be last-writer-wins on the timestamp
    return dynamoClient.updateItem(UpdateItemRequest.builder()
            .tableName(backupTableName)
            .key(Map.of(KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId)))
            .updateExpression("SET %s".formatted(String.join(",", updates)))
            .expressionAttributeNames(expressionAttributeNames)
            .expressionAttributeValues(Map.of(
                ":cdn", AttributeValues.n(BACKUP_CDN),
                ":expiration", AttributeValues.n(refreshTimeSecs)))
            .build())
        .thenApply(result -> tusBackupCredentialGenerator.generateUpload(encodedBackupId, MESSAGE_BACKUP_NAME));
  }

  /**
   * Update the last update timestamps for the backupId in the presentation
   *
   * @param backupUser an already ZK authenticated backup user
   */
  public CompletableFuture<Void> ttlRefresh(final AuthenticatedBackupUser backupUser) {
    if (backupUser.backupTier().compareTo(BackupTier.MESSAGES) < 0) {
      Metrics.counter(ZK_AUTHZ_FAILURE_COUNTER_NAME).increment();
      throw Status.PERMISSION_DENIED
          .withDescription("credential does not support ttl operation")
          .asRuntimeException();
    }
    final long refreshTimeSecs = clock.instant().getEpochSecond();
    // update message backup TTL
    final List<String> updates = new ArrayList<>(Collections.singletonList("#lastRefresh = :expiration"));
    final Map<String, String> expressionAttributeNames = new HashMap<>(Map.of("#lastRefresh", ATTR_LAST_REFRESH));
    if (backupUser.backupTier().compareTo(BackupTier.MEDIA) >= 0) {
      // update media TTL
      expressionAttributeNames.put("#lastMediaRefresh", ATTR_LAST_MEDIA_REFRESH);
      updates.add("#lastMediaRefresh = :expiration");
    }
    return dynamoClient.updateItem(UpdateItemRequest.builder()
            .tableName(backupTableName)
            .key(Map.of(KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId(backupUser))))
            .updateExpression("SET %s".formatted(String.join(",", updates)))
            .expressionAttributeNames(expressionAttributeNames)
            .expressionAttributeValues(Map.of(":expiration", AttributeValues.n(refreshTimeSecs)))
            .build())
        .thenRun(Util.NOOP);
  }

  public record BackupInfo(int cdn, String backupSubdir, String messageBackupKey, Optional<Long> mediaUsedSpace) {}

  /**
   * Retrieve information about the existing backup
   *
   * @param backupUser an already ZK authenticated backup user
   * @return Information about the existing backup
   */
  public CompletableFuture<BackupInfo> backupInfo(final AuthenticatedBackupUser backupUser) {
    if (backupUser.backupTier().compareTo(BackupTier.MESSAGES) < 0) {
      Metrics.counter(ZK_AUTHZ_FAILURE_COUNTER_NAME).increment();
      throw Status.PERMISSION_DENIED.withDescription("credential does not support info operation")
          .asRuntimeException();
    }
    return backupInfoHelper(backupUser);
  }

  private CompletableFuture<BackupInfo> backupInfoHelper(final AuthenticatedBackupUser backupUser) {
    return dynamoClient.getItem(GetItemRequest.builder()
            .tableName(backupTableName)
            .key(Map.of(KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId(backupUser))))
            .projectionExpression("#cdn,#bytesUsed")
            .expressionAttributeNames(Map.of("#cdn", ATTR_CDN, "#bytesUsed", ATTR_MEDIA_BYTES_USED))
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

          return new BackupInfo(cdn, encodeForCdn(hashedBackupId(backupUser)), MESSAGE_BACKUP_NAME, mediaUsed);
        });
  }

  /**
   * Generate credentials that can be used to read from the backup CDN
   *
   * @param backupUser an already ZK authenticated backup user
   * @return A map of headers to include with CDN requests
   */
  public Map<String, String> generateReadAuth(final AuthenticatedBackupUser backupUser) {
    if (backupUser.backupTier().compareTo(BackupTier.MESSAGES) < 0) {
      Metrics.counter(ZK_AUTHZ_FAILURE_COUNTER_NAME).increment();
      throw Status.PERMISSION_DENIED
          .withDescription("credential does not support read auth operation")
          .asRuntimeException();

    }
    final String encodedBackupId = encodeForCdn(hashedBackupId(backupUser));
    return tusBackupCredentialGenerator.readHeaders(encodedBackupId);
  }

  /**
   * Authenticate the ZK anonymous backup credential's presentation
   * <p>
   * This validates:
   * <li> The presentation was for a credential issued by the server </li>
   * <li> The credential is in its redemption window </li>
   * <li> The backup-id matches a previously committed blinded backup-id and server issued receipt level </li>
   * <li> The signature of the credential matches an existing publicKey associated with this backup-id </li>
   *
   * @param presentation A {@link BackupAuthCredentialPresentation}
   * @param signature    An XEd25519 signature of the presentation bytes
   * @return On authentication success, the authenticated backup-id and backup-tier encoded in the presentation
   */
  public CompletableFuture<AuthenticatedBackupUser> authenticateBackupUser(
      final BackupAuthCredentialPresentation presentation,
      final byte[] signature) {
    final byte[] hashedBackupId = hashedBackupId(presentation.getBackupId());
    return dynamoClient.getItem(GetItemRequest.builder()
            .tableName(backupTableName)
            .key(Map.of(KEY_BACKUP_ID_HASH, AttributeValues.b(hashedBackupId)))
            .projectionExpression("#publicKey")
            .expressionAttributeNames(Map.of("#publicKey", ATTR_PUBLIC_KEY))
            .build())
        .thenApply(response -> {
          if (!response.hasItem()) {
            Metrics.counter(ZK_AUTHN_COUNTER_NAME,
                    SUCCESS_TAG_NAME, String.valueOf(false),
                    FAILURE_REASON_TAG_NAME, "missing_public_key")
                .increment();
            throw Status.NOT_FOUND.withDescription("Backup not found").asRuntimeException();
          }
          final byte[] publicKeyBytes = AttributeValues.get(response.item(), ATTR_PUBLIC_KEY)
              .map(AttributeValue::b)
              .map(SdkBytes::asByteArray)
              .orElseThrow(() -> Status.INTERNAL
                  .withDescription("Stored backup missing public key")
                  .asRuntimeException());
          try {
            final ECPublicKey publicKey = new ECPublicKey(publicKeyBytes);
            return new AuthenticatedBackupUser(
                presentation.getBackupId(),
                verifySignatureAndCheckPresentation(presentation, signature, publicKey));
          } catch (InvalidKeyException e) {
            Metrics.counter(ZK_AUTHN_COUNTER_NAME,
                    SUCCESS_TAG_NAME, String.valueOf(false),
                    FAILURE_REASON_TAG_NAME, "invalid_public_key")
                .increment();
            logger.error("Invalid publicKey for backupId hash {}",
                HexFormat.of().formatHex(hashedBackupId), e);
            throw Status.INTERNAL
                .withCause(e)
                .withDescription("Could not deserialize stored public key")
                .asRuntimeException();
          }
        })
        .thenApply(result -> {
          Metrics.counter(ZK_AUTHN_COUNTER_NAME, SUCCESS_TAG_NAME, String.valueOf(true)).increment();
          return result;
        });
  }


  /**
   * Verify the presentation and return the extracted backup tier
   *
   * @param presentation A ZK credential presentation that encodes the backupId and the receipt level of the requester
   * @return The backup tier this presentation supports
   */
  private BackupTier verifySignatureAndCheckPresentation(
      final BackupAuthCredentialPresentation presentation,
      final byte[] signature,
      final ECPublicKey publicKey) {
    if (!publicKey.verifySignature(presentation.serialize(), signature)) {
      Metrics.counter(ZK_AUTHN_COUNTER_NAME,
              SUCCESS_TAG_NAME, String.valueOf(false),
              FAILURE_REASON_TAG_NAME, "signature_validation")
          .increment();
      throw Status.UNAUTHENTICATED
          .withDescription("backup auth credential presentation signature verification failed")
          .asRuntimeException();
    }
    try {
      presentation.verify(clock.instant(), serverSecretParams);
    } catch (VerificationFailedException e) {
      Metrics.counter(ZK_AUTHN_COUNTER_NAME,
              SUCCESS_TAG_NAME, String.valueOf(false),
              FAILURE_REASON_TAG_NAME, "presentation_verification")
          .increment();
      throw Status.UNAUTHENTICATED
          .withDescription("backup auth credential presentation verification failed")
          .withCause(e)
          .asRuntimeException();
    }

    return BackupTier
        .fromReceiptLevel(presentation.getReceiptLevel())
        .orElseThrow(() -> {
          Metrics.counter(ZK_AUTHN_COUNTER_NAME,
                  SUCCESS_TAG_NAME, String.valueOf(false),
                  FAILURE_REASON_TAG_NAME, "invalid_receipt_level")
              .increment();
          return Status.PERMISSION_DENIED.withDescription("invalid receipt level").asRuntimeException();
        });
  }

  private static byte[] hashedBackupId(final AuthenticatedBackupUser backupId) {
    return hashedBackupId(backupId.backupId());
  }

  private static byte[] hashedBackupId(final byte[] backupId) {
    try {
      return Arrays.copyOf(MessageDigest.getInstance("SHA-256").digest(backupId), 16);
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
  }

  private static String encodeForCdn(final byte[] bytes) {
    return Base64.getUrlEncoder().encodeToString(bytes);
  }
}
