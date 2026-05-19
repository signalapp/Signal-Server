/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static java.util.Objects.requireNonNull;

import java.util.HexFormat;
import java.util.UUID;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;

public class RegistrationRecoveryPasswordsManager {

  private final RegistrationRecoveryPasswords registrationRecoveryPasswords;

  public RegistrationRecoveryPasswordsManager(final RegistrationRecoveryPasswords registrationRecoveryPasswords) {
    this.registrationRecoveryPasswords = requireNonNull(registrationRecoveryPasswords);
  }

  public boolean verify(final UUID phoneNumberIdentifier, final byte[] password) {
    return registrationRecoveryPasswords.lookup(phoneNumberIdentifier)
        .filter(hash -> hash.verify(bytesToString(password))).isPresent();
  }

  public boolean store(final UUID phoneNumberIdentifier, final byte[] password) {
    final String token = bytesToString(password);
    final SaltedTokenHash tokenHash = SaltedTokenHash.generateFor(token);

    return registrationRecoveryPasswords.addOrReplace(phoneNumberIdentifier, tokenHash);
  }

  public boolean remove(final UUID phoneNumberIdentifier) {
    return registrationRecoveryPasswords.removeEntry(phoneNumberIdentifier);
  }

  private static String bytesToString(final byte[] bytes) {
    return HexFormat.of().formatHex(bytes);
  }
}
