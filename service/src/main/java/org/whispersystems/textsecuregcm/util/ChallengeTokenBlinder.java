/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.ProviderException;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Base64;
import java.util.Optional;
import java.util.UUID;
import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.whispersystems.textsecuregcm.configuration.ChallengeConfiguration;

public class ChallengeTokenBlinder {

  private record Token(
      UUID uuid,
      Instant timestamp) {
  }

  private static final ObjectMapper mapper = SystemMapper.jsonMapper();
  private final Clock clock;
  private final Duration tokenTtl;
  private final SecureRandom secureRandom = new SecureRandom();
  private final SecretKey blindingKey;

  public ChallengeTokenBlinder(final ChallengeConfiguration config, final Clock clock) {
    this.blindingKey = new SecretKeySpec(config.blindingSecret().value(), "AES");
    this.tokenTtl = config.tokenTtl();
    this.clock = clock;
  }

  public String generateBlindedAccountToken(UUID aci) {

    final Token token = new Token(aci, clock.instant());
    final byte[] serializedToken;
    try {
      serializedToken = mapper.writeValueAsBytes(token);
    } catch (IOException e) { // should really, really never happen
      throw new IllegalArgumentException();
    }

    final byte[] iv = new byte[12];
    secureRandom.nextBytes(iv);
    final GCMParameterSpec parameterSpec = new GCMParameterSpec(128, iv);

    try {
      final Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      cipher.init(Cipher.ENCRYPT_MODE, blindingKey, parameterSpec);
      final byte[] ciphertext = cipher.doFinal(serializedToken);

      final ByteBuffer byteBuffer = ByteBuffer.allocate(iv.length + ciphertext.length);
      byteBuffer.put(iv);
      byteBuffer.put(ciphertext);
      return Base64.getUrlEncoder().withoutPadding().encodeToString(byteBuffer.array());
    } catch (GeneralSecurityException e) {
      throw new IllegalArgumentException(e);
    }
  }

  public Optional<UUID> unblindAccountToken(String token) {
    final byte[] ciphertext;
    try {
      ciphertext = Base64.getUrlDecoder().decode(token);
    } catch (IllegalArgumentException e) {
      return Optional.empty();
    }

    final Token parsedToken;

    try {
      final Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      final GCMParameterSpec parameterSpec = new GCMParameterSpec(128, ciphertext, 0, 12);
      cipher.init(Cipher.DECRYPT_MODE, blindingKey, parameterSpec);

      parsedToken = mapper.readValue(cipher.doFinal(ciphertext, 12, ciphertext.length - 12), Token.class);
    } catch (ProviderException | AEADBadTagException | JsonProcessingException e) {
      // the token doesn't successfully decrypt with this key, it's bogus (or from an older server version or before a key rotation)
      return Optional.empty();
    } catch (IOException | GeneralSecurityException e) { // should never happen
      throw new IllegalArgumentException();
    }

    Instant now = clock.instant();
    Instant intervalStart = now.minus(tokenTtl);
    Instant tokenTime = parsedToken.timestamp();
    if (tokenTime.isAfter(now) || tokenTime.isBefore(intervalStart)) {
      // expired or fraudulently-future token
      return Optional.empty();
    }

    return Optional.of(parsedToken.uuid());
  }

}
