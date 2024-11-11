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

public record SubscriberCredentials(@Nonnull byte[] subscriberBytes,
                             @Nonnull byte[] subscriberUser,
                             @Nonnull byte[] subscriberKey,
                             @Nonnull byte[] hmac,
                             @Nonnull Instant now) {

  public static SubscriberCredentials process(
      Optional<AuthenticatedDevice> authenticatedAccount,
      String subscriberId,
      Clock clock) throws SubscriptionException{
    Instant now = clock.instant();
    if (authenticatedAccount.isPresent()) {
      throw new SubscriptionException.Forbidden("must not use authenticated connection for subscriber operations");
    }
    byte[] subscriberBytes = convertSubscriberIdStringToBytes(subscriberId);
    byte[] subscriberUser = getUser(subscriberBytes);
    byte[] subscriberKey = getKey(subscriberBytes);
    byte[] hmac = computeHmac(subscriberUser, subscriberKey);
    return new SubscriberCredentials(subscriberBytes, subscriberUser, subscriberKey, hmac, now);
  }

  private static byte[] convertSubscriberIdStringToBytes(String subscriberId) throws SubscriptionException.NotFound {
    try {
      byte[] bytes = Base64.getUrlDecoder().decode(subscriberId);
      if (bytes.length != 32) {
        throw new SubscriptionException.NotFound();
      }
      return bytes;
    } catch (IllegalArgumentException e) {
      throw new SubscriptionException.NotFound(e);
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
