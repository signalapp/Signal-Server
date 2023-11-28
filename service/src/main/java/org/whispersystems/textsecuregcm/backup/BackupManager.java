/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import io.grpc.Status;
import io.micrometer.core.instrument.Metrics;
import java.net.URI;
import java.time.Clock;
import java.util.Base64;
import java.util.HexFormat;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;

public class BackupManager {
  private static final Logger logger = LoggerFactory.getLogger(BackupManager.class);

  static final String MESSAGE_BACKUP_NAME = "messageBackup";
  private static final long MAX_TOTAL_BACKUP_MEDIA_BYTES = 1024L * 1024L * 1024L * 50L;
  private static final long MAX_MEDIA_OBJECT_SIZE = 1024L * 1024L * 101L;
  private static final String ZK_AUTHN_COUNTER_NAME = MetricsUtil.name(BackupManager.class, "authentication");
  private static final String ZK_AUTHZ_FAILURE_COUNTER_NAME = MetricsUtil.name(BackupManager.class,
      "authorizationFailure");
  private static final String SUCCESS_TAG_NAME = "success";
  private static final String FAILURE_REASON_TAG_NAME = "reason";

  private final BackupsDb backupsDb;
  private final GenericServerSecretParams serverSecretParams;
  private final Cdn3BackupCredentialGenerator cdn3BackupCredentialGenerator;
  private final RemoteStorageManager remoteStorageManager;
  private final Map<Integer, String> attachmentCdnBaseUris;
  private final Clock clock;


  public BackupManager(
      final BackupsDb backupsDb,
      final GenericServerSecretParams serverSecretParams,
      final Cdn3BackupCredentialGenerator cdn3BackupCredentialGenerator,
      final RemoteStorageManager remoteStorageManager,
      final Map<Integer, String> attachmentCdnBaseUris,
      final Clock clock) {
    this.backupsDb = backupsDb;
    this.serverSecretParams = serverSecretParams;
    this.cdn3BackupCredentialGenerator = cdn3BackupCredentialGenerator;
    this.remoteStorageManager = remoteStorageManager;
    this.clock = clock;
    // strip trailing "/" for easier URI construction
    this.attachmentCdnBaseUris = attachmentCdnBaseUris.entrySet().stream().collect(Collectors.toMap(
        Map.Entry::getKey,
        entry -> StringUtils.removeEnd(entry.getValue(), "/")
    ));
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
    return backupsDb.setPublicKey(presentation.getBackupId(), backupTier, publicKey)
        .exceptionally(ExceptionUtils.exceptionallyHandler(PublicKeyConflictException.class, ex -> {
          Metrics.counter(ZK_AUTHN_COUNTER_NAME,
                  SUCCESS_TAG_NAME, String.valueOf(false),
                  FAILURE_REASON_TAG_NAME, "public_key_conflict")
              .increment();
          throw Status.UNAUTHENTICATED
              .withDescription("public key does not match existing public key for the backup-id")
              .asRuntimeException();
        }));
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
    final String encodedBackupId = encodeBackupIdForCdn(backupUser);

    // this could race with concurrent updates, but the only effect would be last-writer-wins on the timestamp
    return backupsDb
        .addMessageBackup(backupUser)
        .thenApply(result -> cdn3BackupCredentialGenerator.generateUpload(encodedBackupId, MESSAGE_BACKUP_NAME));
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
    // update message backup TTL
    return backupsDb.ttlRefresh(backupUser);
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
    return backupsDb.describeBackup(backupUser)
        .thenApply(backupDescription -> new BackupInfo(
            backupDescription.cdn(),
            encodeBackupIdForCdn(backupUser),
            MESSAGE_BACKUP_NAME,
            backupDescription.mediaUsedSpace()));
  }

  /**
   * Check if there is enough capacity to store the requested amount of media
   *
   * @param backupUser  an already ZK authenticated backup user
   * @param mediaLength the desired number of media bytes to store
   * @return true if mediaLength bytes can be stored
   */
  public CompletableFuture<Boolean> canStoreMedia(final AuthenticatedBackupUser backupUser, final long mediaLength) {
    if (backupUser.backupTier().compareTo(BackupTier.MEDIA) < 0) {
      Metrics.counter(ZK_AUTHZ_FAILURE_COUNTER_NAME).increment();
      throw Status.PERMISSION_DENIED
          .withDescription("credential does not support storing media")
          .asRuntimeException();
    }
    return backupsDb.describeBackup(backupUser)
        .thenApply(info -> info.mediaUsedSpace()
            .filter(usedSpace -> MAX_TOTAL_BACKUP_MEDIA_BYTES - usedSpace >= mediaLength)
            .isPresent());
  }

  public record StorageDescriptor(int cdn, byte[] key) {}

  /**
   * Copy an encrypted object to the backup cdn, adding a layer of encryption
   * <p>
   * Implementation notes: <p> This method guarantees that any object that gets successfully copied to the backup cdn
   * will also have an entry for the user in the database. <p>
   * <p>
   * However, the converse isn't true; there may be entries in the database that have not made it to the cdn. On list,
   * these entries are checked against the cdn and removed.
   *
   * @return A stage that completes successfully with location of the twice-encrypted object on the backup cdn. The
   * returned CompletionStage can be completed exceptionally with the following exceptions.
   * <ul>
   *  <li> {@link InvalidLengthException} If the expectedSourceLength does not match the length of the sourceUri </li>
   *  <li> {@link SourceObjectNotFoundException} If the no object at sourceUri is found </li>
   *  <li> {@link java.io.IOException} If there was a generic IO issue </li>
   * </ul>
   */
  public CompletableFuture<StorageDescriptor> copyToBackup(
      final AuthenticatedBackupUser backupUser,
      final int sourceCdn,
      final String sourceKey,
      final int sourceLength,
      final MediaEncryptionParameters encryptionParameters,
      final byte[] destinationMediaId) {
    if (backupUser.backupTier().compareTo(BackupTier.MEDIA) < 0) {
      Metrics.counter(ZK_AUTHZ_FAILURE_COUNTER_NAME).increment();
      throw Status.PERMISSION_DENIED
          .withDescription("credential does not support storing media")
          .asRuntimeException();
    }
    if (sourceLength > MAX_MEDIA_OBJECT_SIZE) {
      throw Status.INVALID_ARGUMENT
          .withDescription("Invalid sourceObject size")
          .asRuntimeException();
    }

    final MessageBackupUploadDescriptor dst = cdn3BackupCredentialGenerator.generateUpload(
        encodeBackupIdForCdn(backupUser),
        encodeForCdn(destinationMediaId));

    return this.backupsDb
        // Write the ddb updates before actually updating backing storage
        .trackMedia(backupUser, destinationMediaId, sourceLength)

        // copy the objects. On a failure, make a best-effort attempt to reverse the ddb transaction. If cleanup fails
        // the client may be left with some cleanup to do if they don't eventually upload the media id.
        .thenCompose(ignored -> remoteStorageManager
            // actually perform the copy
            .copy(attachmentReadUri(sourceCdn, sourceKey), sourceLength, encryptionParameters, dst)
            // best effort: on failure, untrack the copied media
            .exceptionallyCompose(copyError -> backupsDb.untrackMedia(backupUser, destinationMediaId, sourceLength)
                .thenCompose(ignoredSuccess -> CompletableFuture.failedFuture(copyError))))

        // indicates where the backup was stored
        .thenApply(ignore -> new StorageDescriptor(dst.cdn(), destinationMediaId));

  }

  /**
   * Construct the URI for an attachment with the specified key
   *
   * @param cdn where the attachment is located
   * @param key the attachment key
   * @return A {@link URI} where the attachment can be retrieved
   */
  private URI attachmentReadUri(final int cdn, final String key) {
    final String baseUri = attachmentCdnBaseUris.get(cdn);
    if (baseUri == null) {
      throw Status.INVALID_ARGUMENT.withDescription("Unknown cdn " + cdn).asRuntimeException();
    }
    return URI.create("%s/%s".formatted(baseUri, key));
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
    final String encodedBackupId = encodeBackupIdForCdn(backupUser);
    return cdn3BackupCredentialGenerator.readHeaders(encodedBackupId);
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
    return backupsDb
        .retrievePublicKey(presentation.getBackupId())
        .thenApply(optionalPublicKey -> {
          final byte[] publicKeyBytes = optionalPublicKey
              .orElseThrow(() -> {
                Metrics.counter(ZK_AUTHN_COUNTER_NAME,
                        SUCCESS_TAG_NAME, String.valueOf(false),
                        FAILURE_REASON_TAG_NAME, "missing_public_key")
                    .increment();
                return Status.NOT_FOUND.withDescription("Backup not found").asRuntimeException();
              });
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
                HexFormat.of().formatHex(BackupsDb.hashedBackupId(presentation.getBackupId())), e);
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

  private static String encodeBackupIdForCdn(final AuthenticatedBackupUser backupUser) {
    return encodeForCdn(BackupsDb.hashedBackupId(backupUser.backupId()));
  }

  private static String encodeForCdn(final byte[] bytes) {
    return Base64.getUrlEncoder().encodeToString(bytes);
  }

}
