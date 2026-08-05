/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static java.util.Objects.requireNonNull;

import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.util.Pair;
import software.amazon.awssdk.services.dynamodb.model.TransactWriteItem;

public class PhoneNumberRecoveryPasswordsManager {

  private final PhoneNumberRecoveryPasswords phoneNumberRecoveryPasswords;

  public PhoneNumberRecoveryPasswordsManager(final PhoneNumberRecoveryPasswords phoneNumberRecoveryPasswords) {
    this.phoneNumberRecoveryPasswords = requireNonNull(phoneNumberRecoveryPasswords);
  }

  public boolean verify(final UUID phoneNumberIdentifier, final byte[] password) {
    return phoneNumberRecoveryPasswords.lookup(phoneNumberIdentifier)
        .filter(hash -> hash.verify(bytesToString(password))).isPresent();
  }

  public boolean store(final UUID phoneNumberIdentifier, final byte[] password) {
    final String token = bytesToString(password);
    final SaltedTokenHash tokenHash = SaltedTokenHash.generateFor(token);

    return phoneNumberRecoveryPasswords.addOrReplace(phoneNumberIdentifier, tokenHash);
  }

  public TransactWriteItem buildTransactWriteItemForStorePassword(final UUID phoneNumberIdentifier, final byte[] password) {
    return phoneNumberRecoveryPasswords.buildWriteItemForAddOrReplace(phoneNumberIdentifier, SaltedTokenHash.generateFor(bytesToString(password)));
  }

  public boolean remove(final UUID phoneNumberIdentifier) {
    return phoneNumberRecoveryPasswords.removeEntry(phoneNumberIdentifier);
  }

  public TransactWriteItem buildTransactWriteItemForRemovePassword(final UUID phoneNumberIdentifier) {
    return phoneNumberRecoveryPasswords.buildWriteItemForRemove(phoneNumberIdentifier);
  }

  private static String bytesToString(final byte[] bytes) {
    return HexFormat.of().formatHex(bytes);
  }

  Optional<Pair<SaltedTokenHash, TransactWriteItem>> getPasswordAndWriteItemForMigration(final UUID phoneNumberIdentifier) {
    final Optional<SaltedTokenHash> maybeExistingPassword = phoneNumberRecoveryPasswords.lookup(phoneNumberIdentifier);

    return maybeExistingPassword.map(existingPassword ->
        new Pair<>(existingPassword, phoneNumberRecoveryPasswords.buildConditionCheckForMigration(phoneNumberIdentifier, existingPassword)));
  }
}
