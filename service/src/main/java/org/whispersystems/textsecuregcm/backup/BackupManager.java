/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.util.DataSize;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.Function;
import org.apache.commons.lang3.StringUtils;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialPresentation;
import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.signal.libsignal.zkgroup.backups.BackupLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.attachments.AttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.TusAttachmentGenerator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedBackupUser;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicBackupConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.securevaluerecovery.SecureValueRecoveryClient;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import javax.annotation.Nullable;

public class BackupManager {

  static final String MESSAGE_BACKUP_NAME = "messageBackup";
  public static final long MAX_MESSAGE_BACKUP_OBJECT_SIZE = DataSize.mebibytes(101).toBytes();
  public static final long MAX_MEDIA_OBJECT_SIZE = DataSize.mebibytes(101).toBytes();

  private static final String ZK_AUTHN_COUNTER_NAME = MetricsUtil.name(BackupManager.class, "authentication");
  private static final String ZK_AUTHZ_FAILURE_COUNTER_NAME = MetricsUtil.name(BackupManager.class,
      "authorizationFailure");
  private static final String USAGE_RECALCULATION_COUNTER_NAME = MetricsUtil.name(BackupManager.class,
      "usageRecalculation");
  private static final String DELETE_COUNT_DISTRIBUTION_NAME = MetricsUtil.name(BackupManager.class,
      "deleteCount");
  private static final Timer SYNCHRONOUS_DELETE_TIMER =
      Metrics.timer(MetricsUtil.name(BackupManager.class, "synchronousDelete"));

  private static final String NUM_OBJECTS_SUMMARY_NAME = MetricsUtil.name(BackupManager.class, "numObjects");
  private static final String BYTES_USED_SUMMARY_NAME = MetricsUtil.name(BackupManager.class, "bytesUsed");

  private static final String SUCCESS_TAG_NAME = "success";
  private static final String FAILURE_REASON_TAG_NAME = "reason";

  private static final Logger log = LoggerFactory.getLogger(BackupManager.class);

  private final BackupsDb backupsDb;
  private final GenericServerSecretParams serverSecretParams;
  private final RateLimiters rateLimiters;
  private final TusAttachmentGenerator tusAttachmentGenerator;
  private final Cdn3BackupCredentialGenerator cdn3BackupCredentialGenerator;
  private final RemoteStorageManager remoteStorageManager;
  private final SecureRandom secureRandom = new SecureRandom();
  private final ExternalServiceCredentialsGenerator secureValueRecoveryBCredentialsGenerator;
  private final SecureValueRecoveryClient secureValueRecoveryBClient;
  private final Clock clock;
  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  public BackupManager(
      final BackupsDb backupsDb,
      final GenericServerSecretParams serverSecretParams,
      final RateLimiters rateLimiters,
      final TusAttachmentGenerator tusAttachmentGenerator,
      final Cdn3BackupCredentialGenerator cdn3BackupCredentialGenerator,
      final RemoteStorageManager remoteStorageManager,
      final ExternalServiceCredentialsGenerator secureValueRecoveryBCredentialsGenerator,
      final SecureValueRecoveryClient secureValueRecoveryBClient,
      final Clock clock,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    this.backupsDb = backupsDb;
    this.serverSecretParams = serverSecretParams;
    this.rateLimiters = rateLimiters;
    this.tusAttachmentGenerator = tusAttachmentGenerator;
    this.cdn3BackupCredentialGenerator = cdn3BackupCredentialGenerator;
    this.remoteStorageManager = remoteStorageManager;
    this.secureValueRecoveryBClient = secureValueRecoveryBClient;
    this.clock = clock;
    this.secureValueRecoveryBCredentialsGenerator = secureValueRecoveryBCredentialsGenerator;
    this.dynamicConfigurationManager = dynamicConfigurationManager;
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
  public void setPublicKey(
      final BackupAuthCredentialPresentation presentation,
      final byte[] signature,
      final ECPublicKey publicKey) {

    // Note: this is a special case where we can't validate the presentation signature against the stored public key
    // because we are currently setting it. We check against the provided public key, but we must also verify that
    // there isn't an existing, different stored public key for the backup-id (verified with a condition expression)
    final Pair<BackupCredentialType, BackupLevel> credentialTypeAndBackupLevel =
        verifyPresentation(presentation).verifySignature(signature, publicKey);

    ExceptionUtils.unwrapSupply(
        PublicKeyConflictException.class,
        () -> backupsDb.setPublicKey(presentation.getBackupId(), credentialTypeAndBackupLevel.second(), publicKey).join(),
        _ -> {
          Metrics.counter(ZK_AUTHN_COUNTER_NAME,
                  SUCCESS_TAG_NAME, String.valueOf(false),
                  FAILURE_REASON_TAG_NAME, "public_key_conflict")
              .increment();
          throw Status.UNAUTHENTICATED
              .withDescription("public key does not match existing public key for the backup-id")
              .asRuntimeException();
        });
  }

  /**
   * Create a form that may be used to upload a backup file for the backupId encoded in the presentation.
   * <p>
   * If successful, this also updates the TTL of the backup.
   *
   * @param backupUser an already ZK authenticated backup user
   * @return the upload form
   */
  public BackupUploadDescriptor createMessageBackupUploadDescriptor(
      final AuthenticatedBackupUser backupUser) {
    checkBackupLevel(backupUser, BackupLevel.FREE);
    checkBackupCredentialType(backupUser, BackupCredentialType.MESSAGES);

    // this could race with concurrent updates, but the only effect would be last-writer-wins on the timestamp
    backupsDb.addMessageBackup(backupUser).join();
    return cdn3BackupCredentialGenerator.generateUpload(cdnMessageBackupName(backupUser));
  }

  public BackupUploadDescriptor createTemporaryAttachmentUploadDescriptor(final AuthenticatedBackupUser backupUser)
      throws RateLimitExceededException {
    checkBackupLevel(backupUser, BackupLevel.PAID);
    checkBackupCredentialType(backupUser, BackupCredentialType.MEDIA);

    rateLimiters.forDescriptor(RateLimiters.For.BACKUP_ATTACHMENT).validate(rateLimitKey(backupUser));
    final byte[] bytes = new byte[15];
    secureRandom.nextBytes(bytes);
    final String attachmentKey = Base64.getUrlEncoder().encodeToString(bytes);
    final AttachmentGenerator.Descriptor descriptor = tusAttachmentGenerator.generateAttachment(attachmentKey);
    return new BackupUploadDescriptor(3, attachmentKey, descriptor.headers(), descriptor.signedUploadLocation());
  }

  /**
   * Update the last update timestamps for the backupId in the presentation
   *
   * @param backupUser an already ZK authenticated backup user
   */
  public void ttlRefresh(final AuthenticatedBackupUser backupUser) {
    checkBackupLevel(backupUser, BackupLevel.FREE);
    // update message backup TTL
    final StoredBackupAttributes storedBackupAttributes = backupsDb.ttlRefresh(backupUser).join();
    if (backupUser.credentialType() == BackupCredentialType.MEDIA) {
      final long maxTotalMediaSize =
          dynamicConfigurationManager.getConfiguration().getBackupConfiguration().maxTotalMediaSize();

      // Report that the backup is out of quota if it cannot store a max size media object
      final boolean quotaExhausted = storedBackupAttributes.bytesUsed() >=
          (maxTotalMediaSize - BackupManager.MAX_MEDIA_OBJECT_SIZE);

      final Tags tags = Tags.of(
          UserAgentTagUtil.getPlatformTag(backupUser.userAgent()),
          Tag.of("type", backupUser.credentialType().name()),
          Tag.of("tier", backupUser.backupLevel().name()),
          Tag.of("quotaExhausted", String.valueOf(quotaExhausted)));

      DistributionSummary.builder(NUM_OBJECTS_SUMMARY_NAME)
          .tags(tags)
          .publishPercentileHistogram()
          .register(Metrics.globalRegistry)
          .record(storedBackupAttributes.numObjects());
      DistributionSummary.builder(BYTES_USED_SUMMARY_NAME)
          .tags(tags)
          .publishPercentileHistogram()
          .register(Metrics.globalRegistry)
          .record(storedBackupAttributes.bytesUsed());
    }
  }

  public record BackupInfo(int cdn, String backupSubdir, String mediaSubdir, String messageBackupKey,
                           Optional<Long> mediaUsedSpace) {}

  /**
   * Retrieve information about the existing backup
   *
   * @param backupUser an already ZK authenticated backup user
   * @return Information about the existing backup
   */
  public BackupInfo backupInfo(final AuthenticatedBackupUser backupUser) {
    checkBackupLevel(backupUser, BackupLevel.FREE);
    final BackupsDb.BackupDescription backupDescription = backupsDb.describeBackup(backupUser).join();
    return new BackupInfo(
        backupDescription.cdn(),
        backupUser.backupDir(),
        backupUser.mediaDir(),
        MESSAGE_BACKUP_NAME,
        backupDescription.mediaUsedSpace());
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

    final DynamicBackupConfiguration backupConfiguration =
        dynamicConfigurationManager.getConfiguration().getBackupConfiguration();

    return Mono.fromFuture(() -> allowedCopies(backupUser, toCopy))
        .flatMapMany(quotaResult -> Flux.concat(

            // Perform copies for requests that fit in our quota, first updating the usage. If the copy fails, our
            // estimated quota usage may not be exact since we update usage first. We make a best-effort attempt
            // to undo the usage update if we know that the copied failed for sure.
            Flux.fromIterable(quotaResult.requestsToCopy())

                // Update the usage in reasonable chunk sizes to bound how out of sync our claimed and actual usage gets
                .buffer(backupConfiguration.usageCheckpointCount())
                .concatMap(copyParameters -> {
                  final long quotaToConsume = copyParameters.stream()
                      .mapToLong(CopyParameters::destinationObjectSize)
                      .sum();
                  return Mono
                      .fromFuture(backupsDb.trackMedia(backupUser, copyParameters.size(), quotaToConsume))
                      .thenMany(Flux.fromIterable(copyParameters));
                })

                // Actually perform the copies now that we've updated the quota
                .flatMapSequential(copyParams -> copyToBackup(backupUser, copyParams)
                        .flatMap(copyResult -> switch (copyResult.outcome()) {
                          case SUCCESS -> Mono.just(copyResult);
                          case SOURCE_WRONG_LENGTH, SOURCE_NOT_FOUND, OUT_OF_QUOTA -> Mono
                              .fromFuture(this.backupsDb.trackMedia(backupUser, -1, -copyParams.destinationObjectSize()))
                              .thenReturn(copyResult);
                        }),
                    backupConfiguration.copyConcurrency(), 1),

            // There wasn't enough quota remaining to perform these copies
            Flux.fromIterable(quotaResult.requestsToReject())
                .map(arg -> new CopyResult(CopyResult.Outcome.OUT_OF_QUOTA, arg.destinationMediaId(), null))
        ));
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
   * Determine which copy requests can be performed with the user's remaining quota. This does not update the quota.
   *
   * @param backupUser The user quota to check against
   * @param toCopy     The proposed copy requests
   * @return list of QuotaResult indicating which requests fit into the remaining quota and which requests should be
   * rejected with {@link CopyResult.Outcome#OUT_OF_QUOTA}
   */
  private CompletableFuture<QuotaResult> allowedCopies(
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

    final DynamicBackupConfiguration backupConfiguration =
        dynamicConfigurationManager.getConfiguration().getBackupConfiguration();
    final Duration maxQuotaStaleness = backupConfiguration.maxQuotaStaleness();
    final long maxTotalMediaSize = backupConfiguration.maxTotalMediaSize();

    return backupsDb.getMediaUsage(backupUser)
        .thenComposeAsync(info -> {
          long remainingQuota = maxTotalMediaSize - info.usageInfo().bytesUsed();
          final boolean canStore = remainingQuota >= totalBytesAdded;
          if (canStore || info.lastRecalculationTime().isAfter(clock.instant().minus(maxQuotaStaleness))) {
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
                Metrics.counter(USAGE_RECALCULATION_COUNTER_NAME, Tags.of(
                    UserAgentTagUtil.getPlatformTag(backupUser.userAgent()),
                    Tag.of("usageChanged", String.valueOf(usageChanged))))
                    .increment();
              })
              .thenApply(newUsage -> maxTotalMediaSize - newUsage.bytesUsed());
        })
        .thenApply(remainingQuota -> {
          // Figure out how many of the requested objects fit in the remaining quota
          final int index = indexWhereTotalExceeds(toCopy, CopyParameters::destinationObjectSize,
              remainingQuota);
          return new QuotaResult(toCopy.subList(0, index), toCopy.subList(index, toCopy.size()));
        });
  }

  public record RecalculationResult(UsageInfo oldUsage, UsageInfo newUsage) {}
  public CompletionStage<Optional<RecalculationResult>> recalculateQuota(final StoredBackupAttributes storedBackupAttributes) {
    if (StringUtils.isBlank(storedBackupAttributes.backupDir()) || StringUtils.isBlank(storedBackupAttributes.mediaDir())) {
      return CompletableFuture.completedFuture(Optional.empty());
    }
    final String cdnPath = cdnMediaDirectory(storedBackupAttributes.backupDir(), storedBackupAttributes.mediaDir());
    return this.remoteStorageManager.calculateBytesUsed(cdnPath).thenCompose(usage ->
      backupsDb.setMediaUsage(storedBackupAttributes, usage).thenApply(ignored ->
          Optional.of(new RecalculationResult(
              new UsageInfo(storedBackupAttributes.bytesUsed(), storedBackupAttributes.numObjects()),
              usage))));
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
   * Generate credentials that can be used with SVRB
   *
   * @param backupUser an already ZK authenticated backup user
   * @return the credential that may be used with SVRB
   */
  public ExternalServiceCredentials generateSvrbAuth(final AuthenticatedBackupUser backupUser) {
    checkBackupLevel(backupUser, BackupLevel.FREE);
    // Clients may only use SVRB with their messages backup-id
    checkBackupCredentialType(backupUser, BackupCredentialType.MESSAGES);
    return secureValueRecoveryBCredentialsGenerator.generateFor(svrbIdentifier(backupUser));
  }

  private static String svrbIdentifier(final AuthenticatedBackupUser backupUser) {
    return svrbIdentifier(BackupsDb.hashedBackupId(backupUser.backupId()));
  }

  private static String svrbIdentifier(final byte[] hashedBackupId) {
    return HexFormat.of().formatHex(hashedBackupId);
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
  public ListMediaResult list(
      final AuthenticatedBackupUser backupUser,
      final Optional<String> cursor,
      final int limit) {
    checkBackupLevel(backupUser, BackupLevel.FREE);
    final RemoteStorageManager.ListResult result =
        remoteStorageManager.list(cdnMediaDirectory(backupUser), cursor, limit).toCompletableFuture().join();
    return new ListMediaResult(result
        .objects()
        .stream()
        .map(entry -> new StorageDescriptorWithLength(
            remoteStorageManager.cdnNumber(),
            decodeMediaIdFromCdn(entry.key()),
            entry.length()
        ))
        .toList(),
        result.cursor());
  }

  public void deleteEntireBackup(final AuthenticatedBackupUser backupUser) {
    checkBackupLevel(backupUser, BackupLevel.FREE);

    final int deletionConcurrency =
        dynamicConfigurationManager.getConfiguration().getBackupConfiguration().deletionConcurrency();

    // Clients only include SVRB data with their messages backup-id
    if (backupUser.credentialType() == BackupCredentialType.MESSAGES) {
      secureValueRecoveryBClient.removeData(svrbIdentifier(backupUser)).join();
    }
    try {
      // Try to swap out the backupDir for the user
      backupsDb.scheduleBackupDeletion(backupUser).join();
    } catch (Exception e) {
      final Throwable unwrapped = ExceptionUtils.unwrap(e);
      if (unwrapped instanceof BackupsDb.PendingDeletionException) {
        // If there was already a pending swap, try to delete the cdn objects directly
        SYNCHRONOUS_DELETE_TIMER.record(() -> deletePrefix(backupUser.backupDir(), deletionConcurrency).join());
      } else {
        throw e;
      }
    }
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
    final DynamicBackupConfiguration backupConfiguration =
        dynamicConfigurationManager.getConfiguration().getBackupConfiguration();

    return Flux.usingWhen(

        // Gather usage updates into the UsageBatcher so we don't have to update our backup record on every delete
        Mono.just(new UsageBatcher(backupConfiguration.usageCheckpointCount())),

        // Deletes the objects, returning their former location. Tracks bytes removed so the quota can be updated on
        // completion
        batcher -> Flux.fromIterable(storageDescriptors)

            // Delete the objects, allowing DELETION_CONCURRENCY operations out at a time
            .flatMapSequential(
                sd -> Mono.fromCompletionStage(remoteStorageManager.delete(cdnMediaPath(backupUser, sd.key()))),
                backupConfiguration.deletionConcurrency())
            .zipWithIterable(storageDescriptors)

            // Track how much the remote storage manager indicated was deleted as part of the operation
            .concatMap(deletedBytesAndStorageDescriptor -> {
              final long deletedBytes = deletedBytesAndStorageDescriptor.getT1();
              final StorageDescriptor sd = deletedBytesAndStorageDescriptor.getT2();

              // If it has been a while, perform a checkpoint to make sure our usage doesn't drift too much
              if (batcher.update(-deletedBytes)) {
                final UsageBatcher.UsageUpdate usageUpdate = batcher.getAndReset();
                return Mono
                    .fromFuture(backupsDb.trackMedia(backupUser, usageUpdate.countDelta, usageUpdate.bytesDelta))
                    .doOnError(throwable ->
                        log.warn("Failed to update delta {} after successful delete operation", usageUpdate, throwable))
                    .thenReturn(sd);
              } else {
                return Mono.just(sd);
              }
            }),

        // On cleanup, update the quota using whatever remaining updates were accumulated in the batcher
        batcher -> {
          final UsageBatcher.UsageUpdate update = batcher.getAndReset();
          return Mono
              .fromFuture(backupsDb.trackMedia(backupUser, update.countDelta, update.bytesDelta))
              .doOnError(throwable ->
                  log.warn("Failed to update delta {} after successful delete operation", update, throwable));
        });
  }

  /**
   * Track pending media usage updates. Not thread safe!
   */
  private static class UsageBatcher {

    private final int usageCheckpointCount;
    private long runningCountDelta = 0;
    private long runningBytesDelta = 0;

    UsageBatcher(int usageCheckpointCount) {
      this.usageCheckpointCount = usageCheckpointCount;
    }

    record UsageUpdate(long countDelta, long bytesDelta) {}

    /**
     * Stage a usage update. Returns true when it is time to make a checkpoint
     *
     * @param bytesDelta The amount of bytes that should be tracked as used (or if negative, freed). If the delta is
     *                   non-zero, the count will also be updated.
     * @return true if we should persist the usage
     */
    boolean update(long bytesDelta) {
      this.runningCountDelta += Long.signum(bytesDelta);
      this.runningBytesDelta += bytesDelta;
      return Math.abs(runningCountDelta) >= usageCheckpointCount;
    }

    /**
     * Get the current usage delta, and set the delta to 0
     * @return A {@link UsageUpdate} to apply
     */
    UsageUpdate getAndReset() {
      final UsageUpdate update = new UsageUpdate(runningCountDelta, runningBytesDelta);
      runningCountDelta = 0;
      runningBytesDelta = 0;
      return update;
    }
  }

  private static final ECPublicKey INVALID_PUBLIC_KEY = ECKeyPair.generate().getPublicKey();

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
  public AuthenticatedBackupUser authenticateBackupUser(
      final BackupAuthCredentialPresentation presentation,
      final byte[] signature,
      final String userAgentString) {
    return ExceptionUtils.unwrapSupply(
        StatusRuntimeException.class,
        () -> authenticateBackupUserAsync(presentation, signature, userAgentString).join());
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
   * @return A future that completes with the authenticated backup-id and backup-tier encoded in the presentation
   */
  public CompletableFuture<AuthenticatedBackupUser> authenticateBackupUserAsync(
      final BackupAuthCredentialPresentation presentation,
      final byte[] signature,
      final String userAgentString) {
    final PresentationSignatureVerifier signatureVerifier = verifyPresentation(presentation);

    return backupsDb
        .retrieveAuthenticationData(presentation.getBackupId())
        .thenApply(optionalAuthenticationData -> {
          final UserAgent userAgent = parseUserAgent(userAgentString);
          final BackupsDb.AuthenticationData authenticationData = optionalAuthenticationData
              .orElseGet(() -> {
                Metrics.counter(ZK_AUTHN_COUNTER_NAME, Tags.of(
                        Tag.of(SUCCESS_TAG_NAME, String.valueOf(false)),
                        Tag.of(FAILURE_REASON_TAG_NAME, "missing_public_key"),
                        UserAgentTagUtil.getPlatformTag(userAgent)))
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
              authenticationData.mediaDir(),
              userAgent);
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
   * @return Flux of {@link StoredBackupAttributes} for each backup record in the backups table
   */
  public Flux<StoredBackupAttributes> listBackupAttributes(final int segments) {
    return this.backupsDb.listBackupAttributes(segments);
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
    // Clients only include SVRB data with their messages backup-id
    final CompletableFuture<Void> svrbRemoval = switch(expiredBackup.expirationType()) {
      case ALL -> secureValueRecoveryBClient.removeData(svrbIdentifier(expiredBackup.hashedBackupId()));
      case MEDIA, GARBAGE_COLLECTION ->  CompletableFuture.completedFuture(null);
    };
    return svrbRemoval.thenCompose(_ -> backupsDb.startExpiration(expiredBackup)
        // the deletion operation is effectively single threaded -- it's expected that the caller can increase
        // concurrency by deleting more backups at once, rather than increasing concurrency deleting an individual
        // backup
        .thenCompose(ignored -> deletePrefix(expiredBackup.prefixToDelete(), 1))
        .thenCompose(ignored -> backupsDb.finishExpiration(expiredBackup)));
  }

  /**
   * List and delete all files associated with a prefix
   *
   * @param prefixToDelete The prefix to expire.
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
      Metrics.counter(ZK_AUTHZ_FAILURE_COUNTER_NAME, Tags.of(
              UserAgentTagUtil.getPlatformTag(backupUser.userAgent()),
              Tag.of(FAILURE_REASON_TAG_NAME, "level")))
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

  private static String cdnMediaDirectory(final String backupDir, final String mediaDir) {
    return "%s/%s/".formatted(backupDir, mediaDir);
  }

  private static String cdnMediaDirectory(final AuthenticatedBackupUser backupUser) {
    return cdnMediaDirectory(backupUser.backupDir(), backupUser.mediaDir());
  }

  private static String cdnMediaPath(final AuthenticatedBackupUser backupUser, final byte[] mediaId) {
    return "%s%s".formatted(cdnMediaDirectory(backupUser), encodeMediaIdForCdn(mediaId));
  }

  static String rateLimitKey(final AuthenticatedBackupUser backupUser) {
    return Base64.getEncoder().encodeToString(BackupsDb.hashedBackupId(backupUser.backupId()));
  }

  private static @Nullable UserAgent parseUserAgent(final String userAgentString) {
    try {
      return UserAgentUtil.parseUserAgentString(userAgentString);
    } catch (UnrecognizedUserAgentException e) {
      return null;
    }
  }
}
