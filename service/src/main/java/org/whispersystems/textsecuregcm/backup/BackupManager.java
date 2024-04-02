/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.Status;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.net.URI;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class BackupManager {

  private static final Logger logger = LoggerFactory.getLogger(BackupManager.class);

  static final String MESSAGE_BACKUP_NAME = "messageBackup";
  static final long MAX_TOTAL_BACKUP_MEDIA_BYTES = 1024L * 1024L * 1024L * 50L;
  static final long MAX_MEDIA_OBJECT_SIZE = 1024L * 1024L * 101L;
  // If the last media usage recalculation is over MAX_QUOTA_STALENESS, force a recalculation before quota enforcement.
  static final Duration MAX_QUOTA_STALENESS = Duration.ofDays(1);
  private static final String ZK_AUTHN_COUNTER_NAME = MetricsUtil.name(BackupManager.class, "authentication");
  private static final String ZK_AUTHZ_FAILURE_COUNTER_NAME = MetricsUtil.name(BackupManager.class,
      "authorizationFailure");
  private static final String USAGE_RECALCULATION_COUNTER_NAME = MetricsUtil.name(BackupManager.class,
      "usageRecalculation");
  private static final String DELETE_COUNT_DISTRIBUTION_NAME = MetricsUtil.name(BackupManager.class,
      "deleteCount");

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
    final BackupTier backupTier = verifyPresentation(presentation).verifySignature(signature, publicKey);
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
    // this could race with concurrent updates, but the only effect would be last-writer-wins on the timestamp
    return backupsDb
        .addMessageBackup(backupUser)
        .thenApply(result -> cdn3BackupCredentialGenerator.generateUpload(cdnMessageBackupName(backupUser)));
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

  public record BackupInfo(int cdn, String backupSubdir, String mediaSubdir, String messageBackupKey,
                           Optional<Long> mediaUsedSpace) {}

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
            backupUser.backupDir(),
            backupUser.mediaDir(),
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
    return backupsDb.getMediaUsage(backupUser)
        .thenComposeAsync(info -> {
          final boolean canStore = MAX_TOTAL_BACKUP_MEDIA_BYTES - info.usageInfo().bytesUsed() >= mediaLength;
          if (canStore || info.lastRecalculationTime().isAfter(clock.instant().minus(MAX_QUOTA_STALENESS))) {
            return CompletableFuture.completedFuture(canStore);
          }

          // The user is out of quota, and we have not recently recalculated the user's usage. Double check by doing a
          // hard recalculation before actually forbidding the user from storing additional media.
          return this.remoteStorageManager.calculateBytesUsed(cdnMediaDirectory(backupUser))
              .thenCompose(usage -> backupsDb
                  .setMediaUsage(backupUser, usage)
                  .thenApply(ignored -> usage))
              .whenComplete((newUsage, throwable) -> {
                boolean usageChanged = throwable == null && !newUsage.equals(info.usageInfo());
                Metrics.counter(USAGE_RECALCULATION_COUNTER_NAME, "usageChanged", String.valueOf(usageChanged))
                    .increment();
              })
              .thenApply(newUsage -> MAX_TOTAL_BACKUP_MEDIA_BYTES - newUsage.bytesUsed() >= mediaLength);
        });
  }

  public record StorageDescriptor(int cdn, byte[] key) {}

  public record StorageDescriptorWithLength(int cdn, byte[] key, long length) {}

  /**
   * Copy an encrypted object to the backup cdn, adding a layer of encryption
   * <p>
   * Implementation notes: <p> This method guarantees that any object that gets successfully copied to the backup cdn
   * will also be deducted from the user's quota. </p>
   * <p>
   * However, the converse isn't true. It's possible we may charge the user for media they failed to copy. As a result,
   * the quota may be over reported and it should be recalculated before taking quota enforcement actions.
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

    final URI sourceUri;
    try {
      sourceUri = attachmentReadUri(sourceCdn, sourceKey);
    } catch (IOException e) {
      return CompletableFuture.failedFuture(e);
    }

    final MessageBackupUploadDescriptor dst = cdn3BackupCredentialGenerator.generateUpload(
        cdnMediaPath(backupUser, destinationMediaId));

    final int destinationLength = encryptionParameters.outputSize(sourceLength);
    return this.backupsDb
        // Write the ddb updates before actually updating backing storage
        .trackMedia(backupUser, 1, destinationLength)

        // Actually copy the objects. If the copy fails, our estimated quota usage may not be exact
        .thenComposeAsync(ignored -> remoteStorageManager.copy(sourceUri, sourceLength, encryptionParameters, dst))
        .exceptionallyCompose(throwable -> {
          final Throwable unwrapped = ExceptionUtils.unwrap(throwable);
          if (!(unwrapped instanceof SourceObjectNotFoundException) && !(unwrapped instanceof InvalidLengthException)) {
            throw ExceptionUtils.wrap(unwrapped);
          }
          // In cases where we know the copy fails without writing anything, we can try to restore the user's quota
          return this.backupsDb.trackMedia(backupUser, -1, -destinationLength).whenComplete((ignored, ignoredEx) -> {
            throw ExceptionUtils.wrap(unwrapped);
          });
        })
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
  private URI attachmentReadUri(final int cdn, final String key) throws IOException {
    final String baseUri = attachmentCdnBaseUris.get(cdn);
    if (baseUri == null) {
      throw new SourceObjectNotFoundException("Unknown attachment cdn " + cdn);
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
    return cdn3BackupCredentialGenerator.readHeaders(backupUser.backupDir());
  }


  /**
   * List of media stored for a particular backup id
   *
   * @param media  A page of media entries
   * @param cursor If set, can be passed back to a subsequent list request to resume listing from the previous point
   */
  public record ListMediaResult(List<StorageDescriptorWithLength> media, Optional<String> cursor) {}

  /**
   * List the media stored by the backupUser
   *
   * @param backupUser An already ZK authenticated backup user
   * @param cursor     A cursor returned by a previous call that can be used to resume listing
   * @param limit      The maximum number of list results to return
   * @return A {@link ListMediaResult}
   */
  public CompletionStage<ListMediaResult> list(
      final AuthenticatedBackupUser backupUser,
      final Optional<String> cursor,
      final int limit) {
    if (backupUser.backupTier().compareTo(BackupTier.MESSAGES) < 0) {
      Metrics.counter(ZK_AUTHZ_FAILURE_COUNTER_NAME).increment();
      throw Status.PERMISSION_DENIED
          .withDescription("credential does not support list operation")
          .asRuntimeException();
    }
    return remoteStorageManager.list(cdnMediaDirectory(backupUser), cursor, limit)
        .thenApply(result ->
            new ListMediaResult(
                result
                    .objects()
                    .stream()
                    .map(entry -> new StorageDescriptorWithLength(
                        remoteStorageManager.cdnNumber(),
                        decodeMediaIdFromCdn(entry.key()),
                        entry.length()
                    ))
                    .toList(),
                result.cursor()
            ));
  }


  private sealed interface Either permits DeleteSuccess, DeleteFailure {}

  private record DeleteSuccess(long usage) implements Either {}

  private record DeleteFailure(Throwable e) implements Either {}

  public CompletableFuture<Void> delete(final AuthenticatedBackupUser backupUser,
      final List<StorageDescriptor> storageDescriptors) {
    if (backupUser.backupTier().compareTo(BackupTier.MESSAGES) < 0) {
      Metrics.counter(ZK_AUTHZ_FAILURE_COUNTER_NAME).increment();
      throw Status.PERMISSION_DENIED
          .withDescription("credential does not support list operation")
          .asRuntimeException();
    }

    if (storageDescriptors.stream().anyMatch(sd -> sd.cdn() != remoteStorageManager.cdnNumber())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("unsupported media cdn provided")
          .asRuntimeException();
    }

    return Flux
        .fromIterable(storageDescriptors)

        // Issue deletes for all storage descriptors (proceeds with default flux concurrency)
        .flatMap(descriptor -> Mono.fromCompletionStage(
            remoteStorageManager
                .delete(cdnMediaPath(backupUser, descriptor.key))
                // Squash errors/success into a single type
                .handle((bytesDeleted, throwable) -> throwable != null
                    ? new DeleteFailure(throwable)
                    : new DeleteSuccess(bytesDeleted))
        ))

        // Update backupsDb with the change in usage
        .collectList()
        .<Void>flatMap(eithers -> {
          // count up usage changes
          long totalBytesDeleted = 0;
          long totalCountDeleted = 0;
          final List<Throwable> toThrow = new ArrayList<>();
          for (Either either : eithers) {
            switch (either) {
              case DeleteFailure f:
                toThrow.add(f.e());
                break;
              case DeleteSuccess s when s.usage() > 0:
                totalBytesDeleted += s.usage();
                totalCountDeleted++;
                break;
              default:
                break;
            }
          }
          final Mono<Void> result = toThrow.isEmpty()
              ? Mono.empty()
              : Mono.error(toThrow.stream().reduce((t1, t2) -> {
                t1.addSuppressed(t2);
                return t1;
              }).get());
          return Mono
              .fromCompletionStage(this.backupsDb.trackMedia(backupUser, -totalCountDeleted, -totalBytesDeleted))
              .then(result);
        })
        .toFuture();
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
    final PresentationSignatureVerifier signatureVerifier = verifyPresentation(presentation);
    return backupsDb
        .retrieveAuthenticationData(presentation.getBackupId())
        .thenApply(optionalAuthenticationData -> {
          final BackupsDb.AuthenticationData authenticationData = optionalAuthenticationData
              .orElseThrow(() -> {
                Metrics.counter(ZK_AUTHN_COUNTER_NAME,
                        SUCCESS_TAG_NAME, String.valueOf(false),
                        FAILURE_REASON_TAG_NAME, "missing_public_key")
                    .increment();
                return Status.NOT_FOUND.withDescription("Backup not found").asRuntimeException();
              });
          return new AuthenticatedBackupUser(
              presentation.getBackupId(),
              signatureVerifier.verifySignature(signature, authenticationData.publicKey()),
              authenticationData.backupDir(), authenticationData.mediaDir());
        })
        .thenApply(result -> {
          Metrics.counter(ZK_AUTHN_COUNTER_NAME, SUCCESS_TAG_NAME, String.valueOf(true)).increment();
          return result;
        });
  }

  /**
   * List all backups whose media or messages refresh timestamp are older than the provided purgeTime
   *
   * @param segments  Number of segments to read in parallel from the underlying backup database
   * @param scheduler Scheduler for running downstream operations
   * @param purgeTime If a backup's last message refresh time is strictly before purgeTime, it will be marked as
   *                  requiring full deletion. If only the last refresh time is strictly before purgeTime, it will be
   *                  marked as requiring message deletion. Otherwise, it will not be included in the results.
   * @return Flux of backups that require some deletion action
   */
  public Flux<ExpiredBackup> getExpiredBackups(final int segments, final Scheduler scheduler, final Instant purgeTime) {
    return this.backupsDb.getExpiredBackups(segments, scheduler, purgeTime);
  }

  /**
   * Delete some or all of the objects associated with the backup, and update the backup database.
   *
   * @param expiredBackup The backup to expire. If the {@link ExpiredBackup} is a media expiration, only the media
   *                      objects will be deleted, otherwise all backup objects will be deleted
   * @return A stage that completes when the deletion operation is finished
   */
  public CompletableFuture<Void> expireBackup(final ExpiredBackup expiredBackup) {
    return backupsDb.startExpiration(expiredBackup)
        .thenCompose(ignored -> deletePrefix(expiredBackup.prefixToDelete()))
        .thenCompose(ignored -> backupsDb.finishExpiration(expiredBackup));
  }

  /**
   * List and delete all files associated with a prefix
   *
   * @param prefixToDelete The prefix to expire.
   * @return A stage that completes when all objects with the given prefix have been deleted
   */
  private CompletableFuture<Void> deletePrefix(final String prefixToDelete) {
    if (prefixToDelete.length() != BackupsDb.BACKUP_DIRECTORY_PATH_LENGTH
        && prefixToDelete.length() != BackupsDb.MEDIA_DIRECTORY_PATH_LENGTH) {
      throw new IllegalArgumentException("Unexpected prefix deletion for " + prefixToDelete);
    }
    final String prefix = prefixToDelete + "/";
    return Mono
        .fromCompletionStage(this.remoteStorageManager.list(prefix, Optional.empty(), 1000))
        .expand(listResult -> {
          if (listResult.cursor().isEmpty()) {
            return Mono.empty();
          }
          return Mono.fromCompletionStage(() -> this.remoteStorageManager.list(prefix, listResult.cursor(), 1000));
        })
        .flatMap(listResult -> Flux.fromIterable(listResult.objects()))
        // Delete the objects. concatMap effectively makes the deletion operation single threaded -- it's expected
        // the caller can increase concurrency by deleting more backups at once, rather than increasing concurrency
        // deleting an individual backup
        .concatMap(result -> Mono.fromCompletionStage(() -> remoteStorageManager.delete(prefix + result.key())))
        .count()
        .doOnSuccess(itemsRemoved -> DistributionSummary.builder(DELETE_COUNT_DISTRIBUTION_NAME)
            .publishPercentileHistogram(true)
            .register(Metrics.globalRegistry)
            .record(itemsRemoved))
        .then()
        .toFuture();
  }

  interface PresentationSignatureVerifier {
    BackupTier verifySignature(byte[] signature, ECPublicKey publicKey);
  }

  /**
   * Verify the presentation was issued by us, which should be done before checking the stored public key
   *
   * @param presentation A ZK credential presentation that encodes the backupId and the receipt level of the requester
   * @return A function that can be used to verify a signature provided with the presentation
   */
  private PresentationSignatureVerifier verifyPresentation(final BackupAuthCredentialPresentation presentation) {
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
    return (signature, publicKey) -> {
      if (!publicKey.verifySignature(presentation.serialize(), signature)) {
        Metrics.counter(ZK_AUTHN_COUNTER_NAME,
                SUCCESS_TAG_NAME, String.valueOf(false),
                FAILURE_REASON_TAG_NAME, "signature_validation")
            .increment();
        throw Status.UNAUTHENTICATED
            .withDescription("backup auth credential presentation signature verification failed")
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
    };
  }

  @VisibleForTesting
  static String encodeMediaIdForCdn(final byte[] bytes) {
    return Base64.getUrlEncoder().encodeToString(bytes);
  }

  private static byte[] decodeMediaIdFromCdn(final String base64) {
    return Base64.getUrlDecoder().decode(base64);
  }

  private static String cdnMessageBackupName(final AuthenticatedBackupUser backupUser) {
    return "%s/%s".formatted(backupUser.backupDir(), MESSAGE_BACKUP_NAME);
  }

  private static String cdnMediaDirectory(final AuthenticatedBackupUser backupUser) {
    return "%s/%s/".formatted(backupUser.backupDir(), backupUser.mediaDir());
  }

  private static String cdnMediaPath(final AuthenticatedBackupUser backupUser, final byte[] mediaId) {
    return "%s%s".formatted(cdnMediaDirectory(backupUser), encodeMediaIdForCdn(mediaId));
  }
}
