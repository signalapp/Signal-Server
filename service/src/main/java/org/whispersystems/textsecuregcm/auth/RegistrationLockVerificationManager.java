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
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.RegistrationLockFailure;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.Util;

public class RegistrationLockVerificationManager {

  @VisibleForTesting
  public static final int FAILURE_HTTP_STATUS = 423;

  private static final String LOCKED_ACCOUNT_COUNTER_NAME =
      name(RegistrationLockVerificationManager.class, "lockedAccount");
  private static final String LOCK_REASON_TAG_NAME = "lockReason";
  private static final String ALREADY_LOCKED_TAG_NAME = "alreadyLocked";


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
  public void verifyRegistrationLock(final Account account, @Nullable final String clientRegistrationLock)
      throws RateLimitExceededException, WebApplicationException {

    final StoredRegistrationLock existingRegistrationLock = account.getRegistrationLock();
    final ExternalServiceCredentials existingBackupCredentials =
        backupServiceCredentialGenerator.generateForUuid(account.getUuid());

    if (!existingRegistrationLock.requiresClientRegistrationLock()) {
      return;
    }

    if (!Util.isEmpty(clientRegistrationLock)) {
      rateLimiters.getPinLimiter().validate(account.getNumber());
    }

    final String phoneNumber = account.getNumber();

    if (!existingRegistrationLock.verify(clientRegistrationLock)) {
      // At this point, the client verified ownership of the phone number but doesn’t have the reglock PIN.
      // Freezing the existing account credentials will definitively start the reglock timeout.
      // Until the timeout, the current reglock can still be supplied,
      // along with phone number verification, to restore access.
      /*
      boolean alreadyLocked = existingAccount.hasLockedCredentials();
      Metrics.counter(LOCKED_ACCOUNT_COUNTER_NAME,
              LOCK_REASON_TAG_NAME, "verifiedNumberFailedReglock",
              ALREADY_LOCKED_TAG_NAME, Boolean.toString(alreadyLocked))
          .increment();

      final Account updatedAccount;
      if (!alreadyLocked) {
        updatedAccount = accounts.update(existingAccount, Account::lockAuthenticationCredentials);
      } else {
        updatedAccount = existingAccount;
      }

      List<Long> deviceIds = updatedAccount.getDevices().stream().map(Device::getId).toList();
      clientPresenceManager.disconnectAllPresences(updatedAccount.getUuid(), deviceIds);
      */

      throw new WebApplicationException(Response.status(FAILURE_HTTP_STATUS)
          .entity(new RegistrationLockFailure(existingRegistrationLock.getTimeRemaining(),
              existingRegistrationLock.needsFailureCredentials() ? existingBackupCredentials : null))
          .build());
    }

    rateLimiters.getPinLimiter().clear(phoneNumber);
  }
}
