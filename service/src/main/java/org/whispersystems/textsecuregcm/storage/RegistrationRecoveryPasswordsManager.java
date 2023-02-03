/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static java.util.Objects.requireNonNull;

import java.lang.invoke.MethodHandles;
import java.util.HexFormat;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;

public class RegistrationRecoveryPasswordsManager {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final RegistrationRecoveryPasswords registrationRecoveryPasswords;


  public RegistrationRecoveryPasswordsManager(final RegistrationRecoveryPasswords registrationRecoveryPasswords) {
    this.registrationRecoveryPasswords = requireNonNull(registrationRecoveryPasswords);
  }

  public CompletableFuture<Boolean> verify(final String number, final byte[] password) {
    return registrationRecoveryPasswords.lookup(number)
        .thenApply(maybeHash -> maybeHash.filter(hash -> hash.verify(bytesToString(password))))
        .whenComplete((result, error) -> {
          if (error != null) {
            logger.warn("Failed to lookup Registration Recovery Password", error);
          }
        })
        .thenApply(Optional::isPresent);
  }

  public CompletableFuture<Void> storeForCurrentNumber(final String number, final byte[] password) {
    final String token = bytesToString(password);
    final SaltedTokenHash tokenHash = SaltedTokenHash.generateFor(token);
    return registrationRecoveryPasswords.addOrReplace(number, tokenHash)
        .whenComplete((result, error) -> {
          if (error != null) {
            logger.warn("Failed to store Registration Recovery Password", error);
          }
        });
  }

  public CompletableFuture<Void> removeForNumber(final String number) {
    // remove is a "fire-and-forget" operation,
    // there is no action to be taken on its completion
    return registrationRecoveryPasswords.removeEntry(number)
        .whenComplete((ignored, error) -> {
          if (error != null) {
            logger.warn("Failed to remove Registration Recovery Password", error);
          }
        });
  }

  private static String bytesToString(final byte[] bytes) {
    return HexFormat.of().formatHex(bytes);
  }
}
