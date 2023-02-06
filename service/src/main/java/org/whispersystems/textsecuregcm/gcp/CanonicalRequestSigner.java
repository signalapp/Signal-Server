/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.gcp;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.HexFormat;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nonnull;

public class CanonicalRequestSigner {

  @Nonnull
  private final PrivateKey rsaSigningKey;

  private static final Pattern PRIVATE_KEY_PATTERN =
      Pattern.compile("^-+BEGIN PRIVATE KEY-+\\s*(.+)\\n-+END PRIVATE KEY-+\\s*$", Pattern.DOTALL);

  public CanonicalRequestSigner(@Nonnull String rsaSigningKey) throws IOException, InvalidKeyException, InvalidKeySpecException {
    this.rsaSigningKey = initializeRsaSigningKey(rsaSigningKey);
  }

  public String sign(@Nonnull CanonicalRequest canonicalRequest) {
    return sign(makeStringToSign(canonicalRequest));
  }

  private String makeStringToSign(@Nonnull final CanonicalRequest canonicalRequest) {
    final StringBuilder result = new StringBuilder("GOOG4-RSA-SHA256\n");

    result.append(canonicalRequest.getActiveDatetime()).append('\n');

    result.append(canonicalRequest.getCredentialScope()).append('\n');

    final MessageDigest sha256;
    try {
      sha256 = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
    sha256.update(canonicalRequest.getCanonicalRequest().getBytes(StandardCharsets.UTF_8));
    result.append(HexFormat.of().formatHex(sha256.digest()));

    return result.toString();
  }

  private String sign(@Nonnull String stringToSign) {
    final byte[] signature;
    try {
      final Signature sha256rsa = Signature.getInstance("SHA256WITHRSA");
      sha256rsa.initSign(rsaSigningKey);
      sha256rsa.update(stringToSign.getBytes(StandardCharsets.UTF_8));
      signature = sha256rsa.sign();
    } catch (NoSuchAlgorithmException | InvalidKeyException | SignatureException e) {
      throw new AssertionError(e);
    }
    return HexFormat.of().formatHex(signature);
  }

  private static PrivateKey initializeRsaSigningKey(String rsaSigningKey) throws IOException, InvalidKeyException, InvalidKeySpecException {
    final Matcher matcher = PRIVATE_KEY_PATTERN.matcher(rsaSigningKey);

    if (matcher.matches()) {
      try {
        final KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        final PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(Base64.getMimeDecoder().decode(matcher.group(1)));
        final PrivateKey key = keyFactory.generatePrivate(keySpec);

        testKeyIsValidForSigning(key);
        return key;
      } catch (NoSuchAlgorithmException e) {
        throw new AssertionError(e);
      }
    }

    throw new IOException("Invalid RSA key");
  }

  private static void testKeyIsValidForSigning(PrivateKey key) throws InvalidKeyException {
    final Signature sha256rsa;
    try {
      sha256rsa = Signature.getInstance("SHA256WITHRSA");
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError(e);
    }
    sha256rsa.initSign(key);
  }
}
