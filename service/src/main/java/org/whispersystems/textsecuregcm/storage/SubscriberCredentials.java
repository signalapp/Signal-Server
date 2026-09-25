/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import jakarta.ws.rs.InternalServerErrorException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.purchases.PurchaseException;
import org.whispersystems.textsecuregcm.purchases.PurchaseForbiddenException;
import org.whispersystems.textsecuregcm.purchases.PurchaseNotFoundException;

public record SubscriberCredentials(@Nonnull byte[] subscriberBytes,
                             @Nonnull byte[] subscriberUser,
                             @Nonnull byte[] subscriberKey,
                             @Nonnull byte[] hmac,
                             @Nonnull Instant now) {

  public static SubscriberCredentials process(
      final Optional<AuthenticatedDevice> authenticatedAccount,
      final String subscriberId,
      final Clock clock) throws PurchaseException {
    if (authenticatedAccount.isPresent()) {
      throw new PurchaseForbiddenException("must not use authenticated connection for subscriber operations");
    }
    final byte[] subscriberBytes = convertSubscriberIdStringToBytes(subscriberId);
    return process(subscriberBytes, clock);
  }

  public static SubscriberCredentials process(
      final byte[] subscriberBytes,
      final Clock clock) {
    final Instant now = clock.instant();
    final byte[] subscriberUser = getUser(subscriberBytes);
    final byte[] subscriberKey = getKey(subscriberBytes);
    final byte[] hmac = computeHmac(subscriberUser, subscriberKey);
    return new SubscriberCredentials(subscriberBytes, subscriberUser, subscriberKey, hmac, now);
  }

  private static byte[] convertSubscriberIdStringToBytes(String subscriberId) throws PurchaseNotFoundException {
    try {
      byte[] bytes = Base64.getUrlDecoder().decode(subscriberId);
      if (bytes.length != 32) {
        throw new PurchaseNotFoundException();
      }
      return bytes;
    } catch (IllegalArgumentException e) {
      throw new PurchaseNotFoundException(e);
    }
  }

  private static byte[] getUser(byte[] subscriberBytes) {
    byte[] user = new byte[16];
    System.arraycopy(subscriberBytes, 0, user, 0, user.length);
    return user;
  }

  private static byte[] getKey(byte[] subscriberBytes) {
    byte[] key = new byte[16];
    System.arraycopy(subscriberBytes, 16, key, 0, key.length);
    return key;
  }

  private static byte[] computeHmac(byte[] subscriberUser, byte[] subscriberKey) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(subscriberKey, "HmacSHA256"));
      return mac.doFinal(subscriberUser);
    } catch (NoSuchAlgorithmException | InvalidKeyException e) {
      throw new InternalServerErrorException(e);
    }
  }
}
