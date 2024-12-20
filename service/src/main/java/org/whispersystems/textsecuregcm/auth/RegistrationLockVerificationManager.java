/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.PhoneVerificationRequest;
import org.whispersystems.textsecuregcm.entities.RegistrationLockFailure;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.push.NotPushRegisteredException;
import org.whispersystems.textsecuregcm.push.PushNotificationManager;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.RegistrationRecoveryPasswordsManager;

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
  private static final String CHALLENGED_DEVICE_NOT_PUSH_REGISTERED_COUNTER_NAME =
      name(RegistrationLockVerificationManager.class, "challengedDeviceNotPushRegistered");
  private static final String ALREADY_LOCKED_TAG_NAME = "alreadyLocked";
  private static final String REGISTRATION_LOCK_VERIFICATION_FLOW_TAG_NAME = "flow";
  private static final String REGISTRATION_LOCK_MATCHES_TAG_NAME = "registrationLockMatches";
  private static final String PHONE_VERIFICATION_TYPE_TAG_NAME = "phoneVerificationType";

  private final AccountsManager accounts;
  private final DisconnectionRequestManager disconnectionRequestManager;
  private final ExternalServiceCredentialsGenerator svr2CredentialGenerator;
  private final RateLimiters rateLimiters;
  private final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager;
  private final PushNotificationManager pushNotificationManager;

  public RegistrationLockVerificationManager(
      final AccountsManager accounts,
      final DisconnectionRequestManager disconnectionRequestManager,
      final ExternalServiceCredentialsGenerator svr2CredentialGenerator,
      final RegistrationRecoveryPasswordsManager registrationRecoveryPasswordsManager,
      final PushNotificationManager pushNotificationManager,
      final RateLimiters rateLimiters) {
    this.accounts = accounts;
    this.disconnectionRequestManager = disconnectionRequestManager;
    this.svr2CredentialGenerator = svr2CredentialGenerator;
    this.registrationRecoveryPasswordsManager = registrationRecoveryPasswordsManager;
    this.pushNotificationManager = pushNotificationManager;
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

    if (StringUtils.isNotEmpty(clientRegistrationLock)) {
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
      final Account updatedAccount;
      if (!alreadyLocked) {
        updatedAccount = accounts.update(account, Account::lockAuthTokenHash);
      } else {
        updatedAccount = account;
      }

      // The client often sends an empty registration lock token on the first request
      // and sends an actual token if the server returns a 423 indicating that one is required.
      // This logic accounts for that behavior by not deleting the registration recovery password
      // if the user verified correctly via registration recovery password and sent an empty token.
      // This allows users to re-register via registration recovery password
      // instead of always being forced to fall back to SMS verification.
      if (!phoneVerificationType.equals(PhoneVerificationRequest.VerificationType.RECOVERY_PASSWORD) || clientRegistrationLock != null) {
        registrationRecoveryPasswordsManager.remove(updatedAccount.getIdentifier(IdentityType.PNI));
      }

      final List<Byte> deviceIds = updatedAccount.getDevices().stream().map(Device::getId).toList();
      disconnectionRequestManager.requestDisconnection(updatedAccount.getUuid(), deviceIds);

      try {
        // Send a push notification that prompts the client to attempt login and fail due to locked credentials
        pushNotificationManager.sendAttemptLoginNotification(updatedAccount, "failedRegistrationLock");
      } catch (final NotPushRegisteredException e) {
        Metrics.counter(CHALLENGED_DEVICE_NOT_PUSH_REGISTERED_COUNTER_NAME).increment();
      }

      throw new WebApplicationException(Response.status(FAILURE_HTTP_STATUS)
          .entity(new RegistrationLockFailure(
              existingRegistrationLock.getTimeRemaining().toMillis(),
              svr2FailureCredentials(existingRegistrationLock, updatedAccount)))
          .build());
    }

    rateLimiters.getPinLimiter().clear(phoneNumber);
  }

  private @Nullable ExternalServiceCredentials svr2FailureCredentials(final StoredRegistrationLock existingRegistrationLock, final Account account) {
    if (!existingRegistrationLock.needsFailureCredentials()) {
      return null;
    }
    return svr2CredentialGenerator.generateForUuid(account.getUuid());
  }

}
