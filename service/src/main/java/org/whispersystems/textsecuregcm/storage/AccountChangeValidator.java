/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class AccountChangeValidator {

  private final boolean allowNumberChange;
  private final boolean allowUsernameChange;

  static final AccountChangeValidator GENERAL_CHANGE_VALIDATOR = new AccountChangeValidator(false, false);
  static final AccountChangeValidator NUMBER_CHANGE_VALIDATOR = new AccountChangeValidator(true, false);
  static final AccountChangeValidator USERNAME_CHANGE_VALIDATOR = new AccountChangeValidator(false, true);

  private static final Logger logger = LoggerFactory.getLogger(AccountChangeValidator.class);

  AccountChangeValidator(final boolean allowNumberChange,
      final boolean allowUsernameChange) {

    this.allowNumberChange = allowNumberChange;
    this.allowUsernameChange = allowUsernameChange;
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

    if (!allowUsernameChange) {
      assert updatedAccount.getUsername().equals(originalAccount.getUsername());

      if (!updatedAccount.getUsername().equals(originalAccount.getUsername())) {
        logger.error("Username changed via \"normal\" update; usernames must be changed via setUsername method",
            new RuntimeException());
      }
    }
  }
}
