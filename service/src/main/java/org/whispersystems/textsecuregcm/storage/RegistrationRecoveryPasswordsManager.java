/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static java.util.Objects.requireNonNull;

import java.lang.invoke.MethodHandles;
import java.util.HexFormat;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;

public class RegistrationRecoveryPasswordsManager {

  private static final Logger logger = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final RegistrationRecoveryPasswords registrationRecoveryPasswords;

  public RegistrationRecoveryPasswordsManager(final RegistrationRecoveryPasswords registrationRecoveryPasswords) {
    this.registrationRecoveryPasswords = requireNonNull(registrationRecoveryPasswords);
  }

  public CompletableFuture<Boolean> verify(final UUID phoneNumberIdentifier, final byte[] password) {
    return registrationRecoveryPasswords.lookup(phoneNumberIdentifier)
        .thenApply(maybeHash -> maybeHash.filter(hash -> hash.verify(bytesToString(password))))
        .whenComplete((result, error) -> {
          if (error != null) {
            logger.warn("Failed to lookup Registration Recovery Password", error);
          }
        })
        .thenApply(Optional::isPresent);
  }

  public CompletableFuture<Void> store(final UUID phoneNumberIdentifier, final byte[] password) {
    final String token = bytesToString(password);
    final SaltedTokenHash tokenHash = SaltedTokenHash.generateFor(token);

    return registrationRecoveryPasswords.addOrReplace(phoneNumberIdentifier, tokenHash)
        .whenComplete((result, error) -> {
          if (error != null) {
            logger.warn("Failed to store Registration Recovery Password", error);
          }
        });
  }

  public CompletableFuture<Void> remove(final UUID phoneNumberIdentifier) {
    return registrationRecoveryPasswords.removeEntry(phoneNumberIdentifier)
        .whenComplete((ignored, error) -> {
          if (error instanceof ResourceNotFoundException) {
            // These will naturally happen if a recovery password is already deleted. Since we can remove
            // the recovery password through many flows, we avoid creating log messages for these exceptions
          } else if (error != null) {
            logger.warn("Failed to remove Registration Recovery Password", error);
          }
        });
  }

  private static String bytesToString(final byte[] bytes) {
    return HexFormat.of().formatHex(bytes);
  }
}
