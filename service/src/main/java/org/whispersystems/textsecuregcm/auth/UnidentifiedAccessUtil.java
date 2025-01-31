/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import org.whispersystems.textsecuregcm.storage.Account;
import java.security.MessageDigest;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.stream.IntStream;

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

  /**
   * Checks whether an action (e.g. sending a message or retrieving pre-keys) may be taken on the collection of target
   * accounts by an actor presenting the given combined unidentified access key.
   *
   * @param targetAccounts the accounts on which an actor wishes to take an action
   * @param combinedUnidentifiedAccessKey the unidentified access key presented by the actor
   *
   * @return {@code true} if an actor presenting the given unidentified access key has permission to take an action on
   * the target accounts or {@code false} otherwise
   */
  public static boolean checkUnidentifiedAccess(final Collection<Account> targetAccounts, final byte[] combinedUnidentifiedAccessKey) {
    return MessageDigest.isEqual(getCombinedUnidentifiedAccessKey(targetAccounts), combinedUnidentifiedAccessKey);
  }

  /**
   * Calculates a combined unidentified access key for the given collection of accounts.
   *
   * @param accounts the accounts from which to derive a combined unidentified access key
   * @return a combined unidentified access key
   *
   * @throws IllegalArgumentException if one or more of the given accounts had an unidentified access key with an
   * unexpected length
   */
  public static byte[] getCombinedUnidentifiedAccessKey(final Collection<Account> accounts) {
    return accounts.stream()
        .filter(Predicate.not(Account::isUnrestrictedUnidentifiedAccess))
        .map(account ->
            account.getUnidentifiedAccessKey()
                .filter(b -> b.length == UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH)
                .orElseThrow(IllegalArgumentException::new))
        .reduce(new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH],
            (a, b) -> {
              final byte[] xor = new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH];
              IntStream.range(0, UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH).forEach(i -> xor[i] = (byte) (a[i] ^ b[i]));
              return xor;
            });
  }
}
