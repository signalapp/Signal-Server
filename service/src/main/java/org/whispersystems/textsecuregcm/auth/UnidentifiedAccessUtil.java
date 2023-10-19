/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import org.whispersystems.textsecuregcm.storage.Account;
import java.security.MessageDigest;

public class UnidentifiedAccessUtil {

  public static final int UNIDENTIFIED_ACCESS_KEY_LENGTH = 16;

  private UnidentifiedAccessUtil() {
  }

  /**
   * Checks whether an action (e.g. sending a message or retrieving pre-keys) may be taken on the target account by an
   * actor presenting the given unidentified access key.
   *
   * @param targetAccount the account on which an actor wishes to take an action
   * @param unidentifiedAccessKey the unidentified access key presented by the actor
   *
   * @return {@code true} if an actor presenting the given unidentified access key has permission to take an action on
   * the target account or {@code false} otherwise
   */
  public static boolean checkUnidentifiedAccess(final Account targetAccount, final byte[] unidentifiedAccessKey) {
    return targetAccount.isUnrestrictedUnidentifiedAccess()
        || targetAccount.getUnidentifiedAccessKey()
        .map(targetUnidentifiedAccessKey -> MessageDigest.isEqual(targetUnidentifiedAccessKey, unidentifiedAccessKey))
        .orElse(false);
  }
}
