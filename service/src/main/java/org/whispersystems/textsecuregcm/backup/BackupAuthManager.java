/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import io.grpc.Status;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequest;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialResponse;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.Util;

/**
 * Issues ZK backup auth credentials for authenticated accounts
 * <p>
 * Authenticated callers can create ZK credentials that contain a blinded backup-id, so that they can later use that
 * backup id without the verifier learning that the id is associated with this account.
 * <p>
 * First use {@link #commitBackupId} to provide a blinded backup-id. This is stored in durable storage. Then the caller
 * can use {@link #getBackupAuthCredentials} to retrieve credentials that can subsequently be used to make anonymously
 * authenticated requests against their backup-id.
 */
public class BackupAuthManager {

  private static final Duration MAX_REDEMPTION_DURATION = Duration.ofDays(7);
  final static String BACKUP_EXPERIMENT_NAME = "backup";
  final static String BACKUP_MEDIA_EXPERIMENT_NAME = "backupMedia";

  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;
  private final GenericServerSecretParams serverSecretParams;
  private final Clock clock;
  private final RateLimiters rateLimiters;
  private final AccountsManager accountsManager;

  public BackupAuthManager(
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager,
      final RateLimiters rateLimiters,
      final AccountsManager accountsManager,
      final GenericServerSecretParams serverSecretParams,
      final Clock clock) {
    this.dynamicConfigurationManager = dynamicConfigurationManager;
    this.rateLimiters = rateLimiters;
    this.accountsManager = accountsManager;
    this.serverSecretParams = serverSecretParams;
    this.clock = clock;
  }

  /**
   * Store a credential request containing a blinded backup-id for future use.
   *
   * @param account                  The account using the backup-id
   * @param backupAuthCredentialRequest A request containing the blinded backup-id
   * @return A future that completes when the credentialRequest has been stored
   * @throws RateLimitExceededException If too many backup-ids have been committed
   */
  public CompletableFuture<Void> commitBackupId(final Account account,
      final BackupAuthCredentialRequest backupAuthCredentialRequest) throws RateLimitExceededException {
    if (receiptLevel(account).isEmpty()) {
      throw Status.PERMISSION_DENIED.withDescription("Backups not allowed on account").asRuntimeException();
    }

    byte[] serializedRequest = backupAuthCredentialRequest.serialize();
    byte[] existingRequest = account.getBackupCredentialRequest();
    if (existingRequest != null && MessageDigest.isEqual(serializedRequest, existingRequest)) {
      // No need to update or enforce rate limits, this is the credential that the user has already
      // committed to.
      return CompletableFuture.completedFuture(null);
    }

    rateLimiters.forDescriptor(RateLimiters.For.SET_BACKUP_ID).validate(account.getUuid());

    return this.accountsManager
        .updateAsync(account, acc -> acc.setBackupCredentialRequest(serializedRequest))
        .thenRun(Util.NOOP);
  }

  public record Credential(BackupAuthCredentialResponse credential, Instant redemptionTime) {}

  /**
   * Create a credential for every day between redemptionStart and redemptionEnd
   * <p>
   * This uses a {@link BackupAuthCredentialRequest} previous stored via {@link this#commitBackupId} to generate the
   * credentials.
   *
   * @param account         The account to create the credentials for
   * @param redemptionStart The day (must be truncated to a day boundary) the first credential should be valid
   * @param redemptionEnd   The day (must be truncated to a day boundary) the last credential should be valid
   * @return Credentials and the day on which they may be redeemed
   */
  public CompletableFuture<List<Credential>> getBackupAuthCredentials(
      final Account account,
      final Instant redemptionStart,
      final Instant redemptionEnd) {

    final long receiptLevel = receiptLevel(account).orElseThrow(
        () -> Status.PERMISSION_DENIED.withDescription("Backups not allowed on account").asRuntimeException());

    final Instant startOfDay = clock.instant().truncatedTo(ChronoUnit.DAYS);
    if (redemptionStart.isAfter(redemptionEnd) ||
        redemptionStart.isBefore(startOfDay) ||
        redemptionEnd.isAfter(startOfDay.plus(MAX_REDEMPTION_DURATION)) ||
        !redemptionStart.equals(redemptionStart.truncatedTo(ChronoUnit.DAYS)) ||
        !redemptionEnd.equals(redemptionEnd.truncatedTo(ChronoUnit.DAYS))) {

      throw Status.INVALID_ARGUMENT.withDescription("invalid redemption window").asRuntimeException();
    }

    // fetch the blinded backup-id the account should have previously committed to
    final byte[] committedBytes = account.getBackupCredentialRequest();
    if (committedBytes == null) {
      throw Status.NOT_FOUND.withDescription("No blinded backup-id has been added to the account").asRuntimeException();
    }

    try {
      // create a credential for every day in the requested period
      final BackupAuthCredentialRequest credentialReq = new BackupAuthCredentialRequest(committedBytes);
      return CompletableFuture.completedFuture(Stream
          .iterate(redemptionStart, curr -> curr.plus(Duration.ofDays(1)))
          .takeWhile(redemptionTime -> !redemptionTime.isAfter(redemptionEnd))
          .map(redemption -> new Credential(
              credentialReq.issueCredential(redemption, receiptLevel, serverSecretParams),
              redemption))
          .toList());
    } catch (InvalidInputException e) {
      throw Status.INTERNAL
          .withDescription("Could not deserialize stored request credential")
          .withCause(e)
          .asRuntimeException();
    }
  }

  private Optional<Long> receiptLevel(final Account account) {
    if (inExperiment(BACKUP_MEDIA_EXPERIMENT_NAME, account)) {
      return Optional.of(BackupTier.MEDIA.getReceiptLevel());
    }
    if (inExperiment(BACKUP_EXPERIMENT_NAME, account)) {
      return Optional.of(BackupTier.MESSAGES.getReceiptLevel());
    }
    return Optional.empty();
  }

  private boolean inExperiment(final String experimentName, final Account account) {
    return dynamicConfigurationManager.getConfiguration()
        .getExperimentEnrollmentConfiguration(experimentName)
        .map(config -> config.getEnrolledUuids().contains(account.getUuid()))
        .orElse(false);
  }
}
