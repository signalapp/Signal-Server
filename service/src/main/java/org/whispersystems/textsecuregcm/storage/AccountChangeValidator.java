/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.security.MessageDigest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


class AccountChangeValidator {

  private static final byte[] NO_HASH = new byte[32];

  private final boolean allowNumberChange;
  private final boolean allowUsernameHashChange;

  static final AccountChangeValidator GENERAL_CHANGE_VALIDATOR = new AccountChangeValidator(false, false);
  static final AccountChangeValidator NUMBER_CHANGE_VALIDATOR = new AccountChangeValidator(true, false);
  static final AccountChangeValidator USERNAME_CHANGE_VALIDATOR = new AccountChangeValidator(false, true);

  private static final Logger logger = LoggerFactory.getLogger(AccountChangeValidator.class);

  AccountChangeValidator(final boolean allowNumberChange,
      final boolean allowUsernameHashChange) {

    this.allowNumberChange = allowNumberChange;
    this.allowUsernameHashChange = allowUsernameHashChange;
  }

  public void validateChange(final Account originalAccount, final Account updatedAccount) {
    if (!allowNumberChange) {
      assert updatedAccount.getNumber().equals(originalAccount.getNumber());

      if (!updatedAccount.getNumber().equals(originalAccount.getNumber())) {
        logger.error("Account number changed via \"normal\" update; numbers must be changed via changeNumber method",
            new RuntimeException());
      }

      assert updatedAccount.getPhoneNumberIdentifier().equals(originalAccount.getPhoneNumberIdentifier());

      if (!updatedAccount.getPhoneNumberIdentifier().equals(originalAccount.getPhoneNumberIdentifier())) {
        logger.error(
            "Phone number identifier changed via \"normal\" update; PNIs must be changed via changeNumber method",
            new RuntimeException());
      }
    }

    if (!allowUsernameHashChange) {
      final byte[] updatedAccountUsernameHash = updatedAccount.getUsernameHash().orElse(NO_HASH);
      final byte[] originalAccountUsernameHash = originalAccount.getUsernameHash().orElse(NO_HASH);

      boolean usernameUnchanged = MessageDigest.isEqual(updatedAccountUsernameHash, originalAccountUsernameHash);

      if (!usernameUnchanged) {
        logger.error("Username hash changed via \"normal\" update; username hashes must be changed via reserveUsernameHash and confirmUsernameHash methods",
            new RuntimeException());
      }
      assert usernameUnchanged;
    }
  }
}
