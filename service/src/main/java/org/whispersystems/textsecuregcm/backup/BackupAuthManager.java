/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.StreamSupport;
import javax.annotation.Nullable;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequest;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialResponse;
import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.signal.libsignal.zkgroup.backups.BackupLevel;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.signal.libsignal.zkgroup.receipts.ReceiptSerial;
import org.signal.libsignal.zkgroup.receipts.ServerZkReceiptOperations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.RedemptionRange;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.RedeemedReceiptsManager;

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

  private static final Logger logger = LoggerFactory.getLogger(BackupAuthManager.class);


  final static String BACKUP_MEDIA_EXPERIMENT_NAME = "backupMedia";

  private final ExperimentEnrollmentManager experimentEnrollmentManager;
  private final GenericServerSecretParams serverSecretParams;
  private final ServerZkReceiptOperations serverZkReceiptOperations;
  private final RedeemedReceiptsManager redeemedReceiptsManager;
  private final Clock clock;
  private final RateLimiters rateLimiters;
  private final AccountsManager accountsManager;

  public BackupAuthManager(
      final ExperimentEnrollmentManager experimentEnrollmentManager,
      final RateLimiters rateLimiters,
      final AccountsManager accountsManager,
      final ServerZkReceiptOperations serverZkReceiptOperations,
      final RedeemedReceiptsManager redeemedReceiptsManager,
      final GenericServerSecretParams serverSecretParams,
      final Clock clock) {
    this.experimentEnrollmentManager = experimentEnrollmentManager;
    this.rateLimiters = rateLimiters;
    this.accountsManager = accountsManager;
    this.serverZkReceiptOperations = serverZkReceiptOperations;
    this.redeemedReceiptsManager = redeemedReceiptsManager;
    this.serverSecretParams = serverSecretParams;
    this.clock = clock;
  }

  /**
   * Store credential requests containing blinded backup-ids for future use.
   *
   * @param account                         The account using the backup-id
   * @param device                          The device setting the account backup-id
   * @param messagesBackupCredentialRequest A request containing the blinded backup-id the client will use to upload
   *                                        message backups
   * @param mediaBackupCredentialRequest    A request containing the blinded backup-id the client will use to upload
   *                                        media backups
   * @throws RateLimitExceededException If too many backup-ids have been committed
   */
  public void commitBackupId(
      final Account account,
      final Device device,
      final Optional<BackupAuthCredentialRequest> messagesBackupCredentialRequest,
      final Optional<BackupAuthCredentialRequest> mediaBackupCredentialRequest)
      throws RateLimitExceededException, BackupPermissionException, BackupInvalidArgumentException {
    if (!device.isPrimary()) {
      throw new BackupPermissionException("Only primary device can set backup-id");
    }

    if (messagesBackupCredentialRequest.isEmpty() && mediaBackupCredentialRequest.isEmpty()) {
      throw new BackupInvalidArgumentException("Must set at least one of message/media credential requests");
    }

    final byte[] storedMessageCredentialRequest = account.getBackupCredentialRequest(BackupCredentialType.MESSAGES)
        .orElse(null);
    final byte[] storedMediaCredentialRequest = account.getBackupCredentialRequest(BackupCredentialType.MEDIA)
        .orElse(null);

    // If the provided credential request is null, we want to set to the existing request
    final byte[] targetMessageCredentialRequest = messagesBackupCredentialRequest
        .map(BackupAuthCredentialRequest::serialize)
        .orElse(storedMessageCredentialRequest);
    final byte[] targetMediaCredentialRequest = mediaBackupCredentialRequest
        .map(BackupAuthCredentialRequest::serialize)
        .orElse(storedMediaCredentialRequest);

    final boolean requiresMessageRotation =
        !MessageDigest.isEqual(targetMessageCredentialRequest, storedMessageCredentialRequest);
    final boolean requiresMediaRotation =
        !MessageDigest.isEqual(targetMediaCredentialRequest, storedMediaCredentialRequest);

    if (!requiresMessageRotation && !requiresMediaRotation) {
      // No need to update or enforce rate limits, this is the credential that the user has already
      // committed to.
      return;
    }

    if (requiresMessageRotation) {
      rateLimiters.forDescriptor(RateLimiters.For.SET_BACKUP_ID).validate(account.getUuid());
    }

    if (requiresMediaRotation && hasActiveVoucher(account)) {
      rateLimiters.forDescriptor(RateLimiters.For.SET_PAID_MEDIA_BACKUP_ID).validate(account.getUuid());
    }

    this.accountsManager.update(account, a ->
        a.setBackupCredentialRequests(targetMessageCredentialRequest, targetMediaCredentialRequest));
  }

  public record BackupIdRotationLimit(boolean hasPermitsRemaining, Duration nextPermitAvailable) {}

  public BackupIdRotationLimit checkBackupIdRotationLimit(final Account account) {
    final RateLimiter messagesLimiter = rateLimiters.forDescriptor(RateLimiters.For.SET_BACKUP_ID);
    final RateLimiter mediaLimiter = rateLimiters.forDescriptor(RateLimiters.For.SET_PAID_MEDIA_BACKUP_ID);

    final boolean isPaid = hasActiveVoucher(account);

    final CompletionStage<Boolean> hasSetMessagesPermits =
        messagesLimiter.hasAvailablePermitsAsync(account.getUuid(), 1);
    final CompletionStage<Boolean> hasSetMediaPermits = isPaid
        ? mediaLimiter.hasAvailablePermitsAsync(account.getUuid(), 1)
        : CompletableFuture.completedFuture(true);

    return hasSetMessagesPermits.thenCombine(hasSetMediaPermits, (hasMessage, hasMedia) -> {
      if (hasMedia && hasMessage) {
        return new BackupIdRotationLimit(true, Duration.ZERO);
      } else {
        final Duration timeToNextPermit = Collections.max(Arrays.asList(
            messagesLimiter.config().permitRegenerationDuration(),
            isPaid ? mediaLimiter.config().permitRegenerationDuration() : Duration.ZERO));
        return new BackupIdRotationLimit(false, timeToNextPermit);
      }
    }).toCompletableFuture().join();
  }

  public record Credential(BackupAuthCredentialResponse credential, Instant redemptionTime) {}

  /**
   * Create a credential for every day between redemptionStart and redemptionEnd
   * <p>
   * This uses a {@link BackupAuthCredentialRequest} previous stored via {@link this#commitBackupId} to generate the
   * credentials.
   * <p>
   * If the account has a BackupVoucher allowing access to paid backups, credentials with a redemptionTime before the
   * voucher's expiration will include paid backup access. If the BackupVoucher exists but is already expired, this
   * method will also remove the expired voucher from the account.
   *
   * @param account         The account to create the credentials for
   * @param credentialType  The type of backup credentials to create
   * @param redemptionRange The time range to return credentials for
   * @return Credentials and the day on which they may be redeemed
   */
  public List<Credential> getBackupAuthCredentials(
      final Account account,
      final BackupCredentialType credentialType,
      final RedemptionRange redemptionRange) throws BackupNotFoundException {

    // If the account has an expired payment, clear it before continuing
    if (hasExpiredVoucher(account)) {
      final Account updated = accountsManager.update(account, a -> {
        // Re-check in case we raced with an update
        if (hasExpiredVoucher(a)) {
          a.setBackupVoucher(null);
        }
      });
      return getBackupAuthCredentials(updated, credentialType, redemptionRange);
    }

    // fetch the blinded backup-id the account should have previously committed to
    final byte[] committedBytes = account.getBackupCredentialRequest(credentialType)
        .orElseThrow(() -> new BackupNotFoundException("No blinded backup-id has been added to the account"));

    try {
      final BackupLevel defaultBackupLevel = configuredBackupLevel(account);

      // create a credential for every day in the requested period
      final BackupAuthCredentialRequest credentialReq = new BackupAuthCredentialRequest(committedBytes);
      return StreamSupport.stream(redemptionRange.spliterator(), false)
          .map(redemptionTime -> {
            // Check if the account has a voucher that's good for a certain receiptLevel at redemption time, otherwise
            // use the default receipt level
            final BackupLevel backupLevel = storedBackupLevel(account, redemptionTime).orElse(defaultBackupLevel);
            return new Credential(
                credentialReq.issueCredential(redemptionTime, backupLevel, credentialType, serverSecretParams),
                redemptionTime);
          })
          .toList();
    } catch (InvalidInputException e) {
      throw new UncheckedIOException(new IOException("Could not deserialize stored request credential", e));
    }
  }

  /**
   * Redeem a receipt to enable paid backups on the account.
   *
   * @param account                       The account to enable backups on
   * @param receiptCredentialPresentation A ZK receipt presentation proving payment
   */
  public void redeemReceipt(
      final Account account,
      final ReceiptCredentialPresentation receiptCredentialPresentation)
      throws BackupBadReceiptException, BackupInvalidArgumentException, BackupMissingIdCommitmentException {
    try {
      serverZkReceiptOperations.verifyReceiptCredentialPresentation(receiptCredentialPresentation);
    } catch (VerificationFailedException e) {
      throw new BackupBadReceiptException("receipt credential presentation verification failed");
    }
    final ReceiptSerial receiptSerial = receiptCredentialPresentation.getReceiptSerial();
    final Instant receiptExpiration = Instant.ofEpochSecond(receiptCredentialPresentation.getReceiptExpirationTime());
    if (clock.instant().isAfter(receiptExpiration)) {
      throw new BackupBadReceiptException("receipt is already expired");
    }

    final long receiptLevel = receiptCredentialPresentation.getReceiptLevel();

    if (BackupLevelUtil.fromReceiptLevel(receiptLevel) != BackupLevel.PAID) {
      throw new BackupInvalidArgumentException("server does not recognize the requested receipt level");
    }

    if (account.getBackupCredentialRequest(BackupCredentialType.MEDIA).isEmpty()) {
      throw new BackupMissingIdCommitmentException();
    }

    boolean receiptAllowed = redeemedReceiptsManager
        .put(receiptSerial, receiptExpiration.getEpochSecond(), receiptLevel, account.getUuid())
        .join();
    if (!receiptAllowed) {
      throw new BackupBadReceiptException("receipt serial is already redeemed");
    }
    extendBackupVoucher(account, new Account.BackupVoucher(receiptLevel, receiptExpiration));
  }

  /**
   * Extend the duration of the backup voucher on an account.
   *
   * @param account The account to update
   * @param backupVoucher The backup voucher to apply to this account
   */
  public void extendBackupVoucher(final Account account, final Account.BackupVoucher backupVoucher) {
    accountsManager.update(account, a -> {
      // Receipt credential expirations must be day aligned. Make sure any manually set backupVoucher is also day
      // aligned
      final Account.BackupVoucher newPayment =  new Account.BackupVoucher(
          backupVoucher.receiptLevel(),
          backupVoucher.expiration().truncatedTo(ChronoUnit.DAYS));
      final Account.BackupVoucher existingPayment = a.getBackupVoucher();
      a.setBackupVoucher(merge(existingPayment, newPayment));
    });
  }

  private static Account.BackupVoucher merge(@Nullable final Account.BackupVoucher prev,
      final Account.BackupVoucher next) {
    if (prev == null) {
      return next;
    }

    if (next.receiptLevel() != prev.receiptLevel()) {
      return next;
    }

    // If the new payment has the same receipt level as the old, select the further out of the two expiration times
    if (prev.expiration().isAfter(next.expiration())) {
      // This should be fairly rare, either a client reused an old receipt or we reduced the validity period
      logger.warn(
          "Redeemed receipt with an expiration at {} when we've previously had a redemption with a later expiration {}",
          next.expiration(), prev.expiration());
      return prev;
    }
    return next;
  }

  private boolean hasActiveVoucher(final Account account) {
    return account.getBackupVoucher() != null && clock.instant().isBefore(account.getBackupVoucher().expiration());
  }

  private boolean hasExpiredVoucher(final Account account) {
    return account.getBackupVoucher() != null && !hasActiveVoucher(account);
  }

  /**
   * Get the receipt level stored in the {@link Account.BackupVoucher} on the account if it's present and not expired.
   *
   * @param account        The account to check
   * @param redemptionTime The time to check against the expiration time
   * @return The receipt level on the backup voucher, or empty if the account does not have one or it is expired
   */
  private Optional<BackupLevel> storedBackupLevel(final Account account, final Instant redemptionTime) {
    return Optional.ofNullable(account.getBackupVoucher())
        .filter(backupVoucher -> !redemptionTime.isAfter(backupVoucher.expiration()))
        .map(Account.BackupVoucher::receiptLevel)
        .map(BackupLevelUtil::fromReceiptLevel);
  }

  /**
   * Get the backup receipt level that should be used by default for this account determined via configuration.
   *
   * @param account the account to check
   * @return The default receipt level that should be used for the account if the account does not have a
   * BackupVoucher.
   */
  private BackupLevel configuredBackupLevel(final Account account) {
    return this.experimentEnrollmentManager.isEnrolled(account.getUuid(), BACKUP_MEDIA_EXPERIMENT_NAME)
        ? BackupLevel.PAID
        : BackupLevel.FREE;
  }
}
