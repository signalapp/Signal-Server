/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nullable;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.PhoneVerificationRequest;
import org.whispersystems.textsecuregcm.entities.RegistrationLockFailure;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.Util;
import java.time.Duration;
import java.time.Instant;

public class RegistrationLockVerificationManager {
  public enum Flow {
    REGISTRATION,
    CHANGE_NUMBER
  }

  @VisibleForTesting
  public static final int FAILURE_HTTP_STATUS = 423;

  private static final String EXPIRED_REGISTRATION_LOCK_COUNTER_NAME =
      name(RegistrationLockVerificationManager.class, "expiredRegistrationLock");
  private static final String REQUIRED_REGISTRATION_LOCK_COUNTER_NAME =
      name(RegistrationLockVerificationManager.class, "requiredRegistrationLock");
  private static final String ALREADY_LOCKED_TAG_NAME = "alreadyLocked";
  private static final String REGISTRATION_LOCK_VERIFICATION_FLOW_TAG_NAME = "flow";
  private static final String REGISTRATION_LOCK_MATCHES_TAG_NAME = "registrationLockMatches";
  private static final String PHONE_VERIFICATION_TYPE_TAG_NAME = "phoneVerificationType";


  private final AccountsManager accounts;
  private final ClientPresenceManager clientPresenceManager;
  private final ExternalServiceCredentialsGenerator backupServiceCredentialGenerator;
  private final RateLimiters rateLimiters;

  public RegistrationLockVerificationManager(
      final AccountsManager accounts, final ClientPresenceManager clientPresenceManager,
      final ExternalServiceCredentialsGenerator backupServiceCredentialGenerator, final RateLimiters rateLimiters) {
    this.accounts = accounts;
    this.clientPresenceManager = clientPresenceManager;
    this.backupServiceCredentialGenerator = backupServiceCredentialGenerator;
    this.rateLimiters = rateLimiters;
  }

  /**
   * Verifies the given registration lock credentials against the account’s current registration lock, if any
   *
   * @param account
   * @param clientRegistrationLock
   * @throws RateLimitExceededException
   * @throws WebApplicationException
   */
  public void verifyRegistrationLock(final Account account, @Nullable final String clientRegistrationLock,
      final String userAgent,
      final Flow flow,
      final PhoneVerificationRequest.VerificationType phoneVerificationType
  ) throws RateLimitExceededException, WebApplicationException {

    final Tags expiredTags = Tags.of(UserAgentTagUtil.getPlatformTag(userAgent),
        Tag.of(REGISTRATION_LOCK_VERIFICATION_FLOW_TAG_NAME, flow.name()),
        Tag.of(PHONE_VERIFICATION_TYPE_TAG_NAME, phoneVerificationType.name())
    );

    final StoredRegistrationLock existingRegistrationLock = account.getRegistrationLock();

    switch (existingRegistrationLock.getStatus()) {
      case EXPIRED:
        Metrics.counter(EXPIRED_REGISTRATION_LOCK_COUNTER_NAME, expiredTags).increment();
        return;
      case ABSENT:
        return;
      case REQUIRED:
        break;
      default:
        throw new RuntimeException("Unexpected status: " + existingRegistrationLock.getStatus());
    }

    if (!Util.isEmpty(clientRegistrationLock)) {
      rateLimiters.getPinLimiter().validate(account.getNumber());
    }

    final String phoneNumber = account.getNumber();
    final boolean registrationLockMatches = existingRegistrationLock.verify(clientRegistrationLock);
    final boolean alreadyLocked = account.hasLockedCredentials();

    final Tags additionalTags = expiredTags.and(
        REGISTRATION_LOCK_MATCHES_TAG_NAME, Boolean.toString(registrationLockMatches),
        ALREADY_LOCKED_TAG_NAME, Boolean.toString(alreadyLocked)
    );

    Metrics.counter(REQUIRED_REGISTRATION_LOCK_COUNTER_NAME, additionalTags).increment();

    final DistributionSummary registrationLockIdleDays = DistributionSummary
        .builder(name(RegistrationLockVerificationManager.class, "registrationLockIdleDays"))
        .tags(additionalTags)
        .publishPercentiles(0.75, 0.95, 0.99, 0.999)
        .distributionStatisticExpiry(Duration.ofHours(2))
        .register(Metrics.globalRegistry);

    final Instant accountLastSeen = Instant.ofEpochMilli(account.getLastSeen());
    final Duration timeSinceLastSeen = Duration.between(accountLastSeen, Instant.now());

    registrationLockIdleDays.record(timeSinceLastSeen.toDays());

    if (!registrationLockMatches) {
      // At this point, the client verified ownership of the phone number but doesn’t have the reglock PIN.
      // Freezing the existing account credentials will definitively start the reglock timeout.
      // Until the timeout, the current reglock can still be supplied,
      // along with phone number verification, to restore access.
      /*

      final Account updatedAccount;
      if (!alreadyLocked) {
        updatedAccount = accounts.update(existingAccount, Account::lockAuthenticationCredentials);
      } else {
        updatedAccount = existingAccount;
      }

      List<Long> deviceIds = updatedAccount.getDevices().stream().map(Device::getId).toList();
      clientPresenceManager.disconnectAllPresences(updatedAccount.getUuid(), deviceIds);
      */
      final ExternalServiceCredentials existingBackupCredentials =
          backupServiceCredentialGenerator.generateForUuid(account.getUuid());

      throw new WebApplicationException(Response.status(FAILURE_HTTP_STATUS)
          .entity(new RegistrationLockFailure(existingRegistrationLock.getTimeRemaining().toMillis(),
              existingRegistrationLock.needsFailureCredentials() ? existingBackupCredentials : null))
          .build());
    }

    rateLimiters.getPinLimiter().clear(phoneNumber);
  }
}
