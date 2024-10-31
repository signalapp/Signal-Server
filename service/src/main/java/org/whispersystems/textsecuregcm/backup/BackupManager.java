/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.util.DataSize;
import io.grpc.Status;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.signal.libsignal.zkgroup.backups.BackupLevel;
import org.whispersystems.textsecuregcm.attachments.AttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.TusAttachmentGenerator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.AsyncTimerUtil;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.Pair;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

public class BackupManager {

  static final String MESSAGE_BACKUP_NAME = "messageBackup";
  public static final long MAX_TOTAL_BACKUP_MEDIA_BYTES = DataSize.gibibytes(100).toBytes();
  static final long MAX_MEDIA_OBJECT_SIZE = DataSize.mebibytes(101).toBytes();

  // If the last media usage recalculation is over MAX_QUOTA_STALENESS, force a recalculation before quota enforcement.
  static final Duration MAX_QUOTA_STALENESS = Duration.ofDays(1);

  // How many cdn object deletion requests can be outstanding at a time per backup deletion operation
  private static final int DELETION_CONCURRENCY = 10;

  // How many cdn object copy requests can be outstanding at a time per batch copy-to-backup operation
  private static final int COPY_CONCURRENCY = 10;


  private static final String ZK_AUTHN_COUNTER_NAME = MetricsUtil.name(BackupManager.class, "authentication");
  private static final String ZK_AUTHZ_FAILURE_COUNTER_NAME = MetricsUtil.name(BackupManager.class,
      "authorizationFailure");
  private static final String USAGE_RECALCULATION_COUNTER_NAME = MetricsUtil.name(BackupManager.class,
      "usageRecalculation");
  private static final String DELETE_COUNT_DISTRIBUTION_NAME = MetricsUtil.name(BackupManager.class,
      "deleteCount");
  private static final Timer SYNCHRONOUS_DELETE_TIMER =
      Metrics.timer(MetricsUtil.name(BackupManager.class, "synchronousDelete"));

  private static final String SUCCESS_TAG_NAME = "success";
  private static final String FAILURE_REASON_TAG_NAME = "reason";

  private final BackupsDb backupsDb;
  private final GenericServerSecretParams serverSecretParams;
  private final RateLimiters rateLimiters;
  private final TusAttachmentGenerator tusAttachmentGenerator;
  private final Cdn3BackupCredentialGenerator cdn3BackupCredentialGenerator;
  private final RemoteStorageManager remoteStorageManager;
  private final SecureRandom secureRandom = new SecureRandom();
  private final Clock clock;


  public BackupManager(
      final BackupsDb backupsDb,
      final GenericServerSecretParams serverSecretParams,
      final RateLimiters rateLimiters,
      final TusAttachmentGenerator tusAttachmentGenerator,
      final Cdn3BackupCredentialGenerator cdn3BackupCredentialGenerator,
      final RemoteStorageManager remoteStorageManager,
      final Clock clock) {
    this.backupsDb = backupsDb;
    this.serverSecretParams = serverSecretParams;
    this.rateLimiters = rateLimiters;
    this.tusAttachmentGenerator = tusAttachmentGenerator;
    this.cdn3BackupCredentialGenerator = cdn3BackupCredentialGenerator;
    this.remoteStorageManager = remoteStorageManager;
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
    final Pair<BackupCredentialType, BackupLevel> credentialTypeAndBackupLevel =
        verifyPresentation(presentation).verifySignature(signature, publicKey);

    return backupsDb.setPublicKey(presentation.getBackupId(), credentialTypeAndBackupLevel.second(), publicKey)
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
  public CompletableFuture<BackupUploadDescriptor> createMessageBackupUploadDescriptor(
      final AuthenticatedBackupUser backupUser) {
    checkBackupLevel(backupUser, BackupLevel.FREE);
    checkBackupCredentialType(backupUser, BackupCredentialType.MESSAGES);

    // this could race with concurrent updates, but the only effect would be last-writer-wins on the timestamp
    return backupsDb
        .addMessageBackup(backupUser)
        .thenApply(result -> cdn3BackupCredentialGenerator.generateUpload(cdnMessageBackupName(backupUser)));
  }

  public CompletableFuture<BackupUploadDescriptor> createTemporaryAttachmentUploadDescriptor(
      final AuthenticatedBackupUser backupUser) {
    checkBackupLevel(backupUser, BackupLevel.PAID);
    checkBackupCredentialType(backupUser, BackupCredentialType.MEDIA);

    return rateLimiters.forDescriptor(RateLimiters.For.BACKUP_ATTACHMENT)
        .validateAsync(rateLimitKey(backupUser)).thenApply(ignored -> {
      final byte[] bytes = new byte[15];
      secureRandom.nextBytes(bytes);
      final String attachmentKey = Base64.getUrlEncoder().encodeToString(bytes);
      final AttachmentGenerator.Descriptor descriptor = tusAttachmentGenerator.generateAttachment(attachmentKey);
      return new BackupUploadDescriptor(3, attachmentKey, descriptor.headers(), descriptor.signedUploadLocation());
    }).toCompletableFuture();
  }

  /**
   * Update the last update timestamps for the backupId in the presentation
   *
   * @param backupUser an already ZK authenticated backup user
   */
  public CompletableFuture<Void> ttlRefresh(final AuthenticatedBackupUser backupUser) {
    checkBackupLevel(backupUser, BackupLevel.FREE);
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
    checkBackupLevel(backupUser, BackupLevel.FREE);
    return backupsDb.describeBackup(backupUser)
        .thenApply(backupDescription -> new BackupInfo(
            backupDescription.cdn(),
            backupUser.backupDir(),
            backupUser.mediaDir(),
            MESSAGE_BACKUP_NAME,
            backupDescription.mediaUsedSpace()));
  }

  /**
   * Copy an encrypted object to the backup cdn, adding a layer of encryption
   * <p>
   * Implementation notes: <p> This method guarantees that any object that gets successfully copied to the backup cdn
   * will also be deducted from the user's quota. </p>
   * <p>
   * However, the converse isn't true. It's possible we may charge the user for media they failed to copy. As a result,
   * the quota may be over reported. It should be recalculated before taking quota enforcement actions.
   *
   * @return A Flux that emits the locations of the double-encrypted objects on the backup cdn, or includes an error
   * detailing why the object could not be copied.
   */
  public Flux<CopyResult> copyToBackup(final AuthenticatedBackupUser backupUser, List<CopyParameters> toCopy) {
    checkBackupLevel(backupUser, BackupLevel.PAID);
    checkBackupCredentialType(backupUser, BackupCredentialType.MEDIA);

    return Mono
        // Figure out how many objects we're allowed to copy, updating the quota usage for the amount we are allowed
        .fromFuture(enforceQuota(backupUser, toCopy))

        // Copy the ones we have enough quota to hold
        .flatMapMany(quotaResult -> Flux.concat(

            // These fit in our remaining quota, so perform the copy. If the copy fails, our estimated quota usage may not
            // be exact since we already updated our usage. We make a best-effort attempt to undo the usage update if we
            // know that the copied failed for sure though.
            Flux.fromIterable(quotaResult.requestsToCopy()).flatMapSequential(
                copyParams -> copyToBackup(backupUser, copyParams)
                    .flatMap(copyResult -> switch (copyResult.outcome()) {
                      case SUCCESS -> Mono.just(copyResult);
                      case SOURCE_WRONG_LENGTH, SOURCE_NOT_FOUND, OUT_OF_QUOTA -> Mono
                          .fromFuture(this.backupsDb.trackMedia(backupUser, -1, -copyParams.destinationObjectSize()))
                          .thenReturn(copyResult);
                    }),
                COPY_CONCURRENCY),

            // There wasn't enough quota remaining to perform these copies
            Flux.fromIterable(quotaResult.requestsToReject())
                .map(arg -> new CopyResult(CopyResult.Outcome.OUT_OF_QUOTA, arg.destinationMediaId(), null))));
  }

  private Mono<CopyResult> copyToBackup(final AuthenticatedBackupUser backupUser, final CopyParameters copyParameters) {
    return Mono.fromCompletionStage(() -> remoteStorageManager.copy(
            copyParameters.sourceCdn(), copyParameters.sourceKey(), copyParameters.sourceLength(),
            copyParameters.encryptionParameters(),
            cdnMediaPath(backupUser, copyParameters.destinationMediaId())))

        // Successfully copied!
        .thenReturn(new CopyResult(
            CopyResult.Outcome.SUCCESS, copyParameters.destinationMediaId(), remoteStorageManager.cdnNumber()))

        // Otherwise, squash per-item copy errors that don't fail the entire operation
        .onErrorResume(
            // If the error maps to an explicit result type
            throwable ->
                CopyResult.fromCopyError(throwable, copyParameters.destinationMediaId()).isPresent(),
            // return that result type instead of propagating the error
            throwable ->
                Mono.just(CopyResult.fromCopyError(throwable, copyParameters.destinationMediaId()).orElseThrow()));
  }

  private record QuotaResult(List<CopyParameters> requestsToCopy, List<CopyParameters> requestsToReject) {}

  /**
   * Determine which copy requests can be performed with the user's remaining quota and update the used quota. If a copy
   * request subsequently fails, the caller should attempt to restore the quota for the failed copy.
   *
   * @param backupUser The user quota to update
   * @param toCopy     The proposed copy requests
   * @return QuotaResult indicating which requests fit into the remaining quota and which requests should be rejected
   * with {@link CopyResult.Outcome#OUT_OF_QUOTA}
   */
  private CompletableFuture<QuotaResult> enforceQuota(
      final AuthenticatedBackupUser backupUser,
      final List<CopyParameters> toCopy) {
    final long totalBytesAdded = toCopy.stream()
        .mapToLong(copyParameters -> {
          if (copyParameters.sourceLength() > MAX_MEDIA_OBJECT_SIZE || copyParameters.sourceLength() < 0) {
            throw Status.INVALID_ARGUMENT
                .withDescription("Invalid sourceObject size")
                .asRuntimeException();
          }
          return copyParameters.destinationObjectSize();
        })
        .sum();

    return backupsDb.getMediaUsage(backupUser)
        .thenComposeAsync(info -> {
          long remainingQuota = MAX_TOTAL_BACKUP_MEDIA_BYTES - info.usageInfo().bytesUsed();
          final boolean canStore = remainingQuota >= totalBytesAdded;
          if (canStore || info.lastRecalculationTime().isAfter(clock.instant().minus(MAX_QUOTA_STALENESS))) {
            return CompletableFuture.completedFuture(remainingQuota);
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
              .thenApply(newUsage -> MAX_TOTAL_BACKUP_MEDIA_BYTES - newUsage.bytesUsed());
        })
        .thenCompose(remainingQuota -> {
          // Figure out how many of the requested objects fit in the remaining quota
          final int index = indexWhereTotalExceeds(toCopy, CopyParameters::destinationObjectSize,
              remainingQuota);
          final QuotaResult result = new QuotaResult(toCopy.subList(0, index),
              toCopy.subList(index, toCopy.size()));
          if (index == 0) {
            // Skip the usage update if we're not able to write anything
            return CompletableFuture.completedFuture(result);
          }

          // Update the usage
          final long quotaToConsume = result.requestsToCopy.stream()
              .mapToLong(CopyParameters::destinationObjectSize)
              .sum();
          return backupsDb.trackMedia(backupUser, index, quotaToConsume).thenApply(ignored -> result);
        });
  }

  /**
   * @return the largest index i such that sum(ts[0],...ts[i - 1]) <= max
   */
  private static <T> int indexWhereTotalExceeds(List<T> ts, Function<T, Long> valueFunction, long max) {
    long sum = 0;
    for (int index = 0; index < ts.size(); index++) {
      sum += valueFunction.apply(ts.get(index));
      if (sum > max) {
        return index;
      }
    }
    return ts.size();
  }


  public record StorageDescriptor(int cdn, byte[] key) {}

  public record StorageDescriptorWithLength(int cdn, byte[] key, long length) {}

  /**
   * Generate credentials that can be used to read from the backup CDN
   *
   * @param backupUser an already ZK authenticated backup user
   * @param cdnNumber  the cdn number to get backup credentials for
   * @return A map of headers to include with CDN requests
   */
  public Map<String, String> generateReadAuth(final AuthenticatedBackupUser backupUser, final int cdnNumber) {
    checkBackupLevel(backupUser, BackupLevel.FREE);
    if (cdnNumber != 3) {
      throw Status.INVALID_ARGUMENT.withDescription("unknown cdn").asRuntimeException();
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
    checkBackupLevel(backupUser, BackupLevel.FREE);
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

  public CompletableFuture<Void> deleteEntireBackup(final AuthenticatedBackupUser backupUser) {
    checkBackupLevel(backupUser, BackupLevel.FREE);
    return backupsDb
        // Try to swap out the backupDir for the user
        .scheduleBackupDeletion(backupUser)
        // If there was already a pending swap, try to delete the cdn objects directly
        .exceptionallyCompose(ExceptionUtils.exceptionallyHandler(BackupsDb.PendingDeletionException.class, e ->
            AsyncTimerUtil.record(SYNCHRONOUS_DELETE_TIMER, () ->
                deletePrefix(backupUser.backupDir(), DELETION_CONCURRENCY))));
  }


  public Flux<StorageDescriptor> deleteMedia(final AuthenticatedBackupUser backupUser,
      final List<StorageDescriptor> storageDescriptors) {
    checkBackupLevel(backupUser, BackupLevel.FREE);
    checkBackupCredentialType(backupUser, BackupCredentialType.MEDIA);

    // Check for a cdn we don't know how to process
    if (storageDescriptors.stream().anyMatch(sd -> sd.cdn() != remoteStorageManager.cdnNumber())) {
      throw Status.INVALID_ARGUMENT
          .withDescription("unsupported media cdn provided")
          .asRuntimeException();
    }

    return Flux.usingWhen(

        // Gather usage updates into the UsageBatcher to apply during the cleanup operation
        Mono.just(new UsageBatcher()),

        // Deletes the objects, returning their former location. Tracks bytes removed so the quota can be updated on
        // completion
        batcher -> Flux.fromIterable(storageDescriptors)
            .flatMapSequential(sd -> Mono
                // Delete the object
                .fromCompletionStage(remoteStorageManager.delete(cdnMediaPath(backupUser, sd.key())))
                // Track how much the remote storage manager indicated was deleted as part of the operation
                .doOnNext(deletedBytes -> batcher.update(-deletedBytes))
                .thenReturn(sd), DELETION_CONCURRENCY),

        // On cleanup, update the quota using whatever updates were accumulated in the batcher
        batcher ->
            Mono.fromFuture(backupsDb.trackMedia(backupUser, batcher.countDelta.get(), batcher.usageDelta.get())));
  }

  /**
   * Track pending media usage updates
   */
  private static class UsageBatcher {

    AtomicLong countDelta = new AtomicLong();
    AtomicLong usageDelta = new AtomicLong();

    /**
     * Stage a usage update that will be applied later
     *
     * @param bytesDelta The amount of bytes that should be tracked as used (or if negative, freed). If the delta is
     *                   non-zero, the count will also be updated.
     */
    void update(long bytesDelta) {
      if (bytesDelta < 0) {
        countDelta.decrementAndGet();
      } else if (bytesDelta > 0) {
        countDelta.incrementAndGet();
      }
      usageDelta.addAndGet(bytesDelta);
    }
  }

  private static final ECPublicKey INVALID_PUBLIC_KEY = Curve.generateKeyPair().getPublicKey();

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
              .orElseGet(() -> {
                Metrics.counter(ZK_AUTHN_COUNTER_NAME,
                        SUCCESS_TAG_NAME, String.valueOf(false),
                        FAILURE_REASON_TAG_NAME, "missing_public_key")
                    .increment();
                // There was no stored public key, use a bunk public key so that validation will fail
                return new BackupsDb.AuthenticationData(INVALID_PUBLIC_KEY, null, null);
              });

          final Pair<BackupCredentialType, BackupLevel> credentialTypeAndBackupLevel =
              signatureVerifier.verifySignature(signature, authenticationData.publicKey());

          return new AuthenticatedBackupUser(
              presentation.getBackupId(),
              credentialTypeAndBackupLevel.first(),
              credentialTypeAndBackupLevel.second(),
              authenticationData.backupDir(),
              authenticationData.mediaDir());
        })
        .thenApply(result -> {
          Metrics.counter(ZK_AUTHN_COUNTER_NAME, SUCCESS_TAG_NAME, String.valueOf(true)).increment();
          return result;
        });
  }

  /**
   * List all backups stored in the backups table
   *
   * @param segments  Number of segments to read in parallel from the underlying backup database
   * @param scheduler Scheduler for running downstream operations
   * @return Flux of {@link StoredBackupAttributes} for each backup record in the backups table
   */
  public Flux<StoredBackupAttributes> listBackupAttributes(final int segments, final Scheduler scheduler) {
    return this.backupsDb.listBackupAttributes(segments, scheduler);
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
        // the deletion operation is effectively single threaded -- it's expected that the caller can increase
        // concurrency by deleting more backups at once, rather than increasing concurrency deleting an individual
        // backup
        .thenCompose(ignored -> deletePrefix(expiredBackup.prefixToDelete(), 1))
        .thenCompose(ignored -> backupsDb.finishExpiration(expiredBackup));
  }

  /**
   * List and delete all files associated with a prefix
   *
   * @param prefixToDelete The prefix to expire.
   * @return A stage that completes when all objects with the given prefix have been deleted
   */
  private CompletableFuture<Void> deletePrefix(final String prefixToDelete, int concurrentDeletes) {
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
        .flatMap(
            result -> Mono.fromCompletionStage(() -> remoteStorageManager.delete(prefix + result.key())),
            concurrentDeletes)
        .count()
        .doOnSuccess(itemsRemoved -> DistributionSummary.builder(DELETE_COUNT_DISTRIBUTION_NAME)
            .publishPercentileHistogram(true)
            .register(Metrics.globalRegistry)
            .record(itemsRemoved))
        .then()
        .toFuture();
  }

  interface PresentationSignatureVerifier {

    Pair<BackupCredentialType, BackupLevel> verifySignature(byte[] signature, ECPublicKey publicKey);
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
      return new Pair<>(presentation.getType(), presentation.getBackupLevel());
    };
  }

  /**
   * Check that the authenticated backup user is authorized to use the provided backupLevel
   *
   * @param backupUser  The backup user to check
   * @param backupLevel The authorization level to verify the backupUser has access to
   * @throws {@link Status#PERMISSION_DENIED} error if the backup user is not authorized to access {@code backupLevel}
   */
  @VisibleForTesting
  static void checkBackupLevel(final AuthenticatedBackupUser backupUser, final BackupLevel backupLevel) {
    if (backupUser.backupLevel().compareTo(backupLevel) < 0) {
      Metrics.counter(ZK_AUTHZ_FAILURE_COUNTER_NAME,
              FAILURE_REASON_TAG_NAME, "level")
          .increment();

      throw Status.PERMISSION_DENIED
          .withDescription("credential does not support the requested operation")
          .asRuntimeException();
    }
  }

  /**
   * Check that the authenticated backup user is authenticated with the given credential type
   *
   * @param backupUser     The backup user to check
   * @param credentialType The credential type to require
   * @throws {@link Status#UNAUTHENTICATED} error if the backup user is not authenticated with the given
   * {@code credentialType}
   */
  @VisibleForTesting
  static void checkBackupCredentialType(final AuthenticatedBackupUser backupUser, final BackupCredentialType credentialType) {
    if (backupUser.credentialType() != credentialType) {
      Metrics.counter(ZK_AUTHZ_FAILURE_COUNTER_NAME,
              FAILURE_REASON_TAG_NAME, "credential_type")
          .increment();

      throw Status.UNAUTHENTICATED
          .withDescription("wrong credential type for the requested operation")
          .asRuntimeException();
    }
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

  static String rateLimitKey(final AuthenticatedBackupUser backupUser) {
    return Base64.getEncoder().encodeToString(BackupsDb.hashedBackupId(backupUser.backupId()));
  }
}
