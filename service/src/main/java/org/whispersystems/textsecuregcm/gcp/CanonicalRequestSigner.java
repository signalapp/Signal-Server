package org.whispersystems.textsecuregcm.gcp;

import org.apache.commons.codec.binary.Hex;
import org.bouncycastle.openssl.PEMReader;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;

public class CanonicalRequestSigner {

  @Nonnull
  private final PrivateKey rsaSigningKey;

  public CanonicalRequestSigner(@Nonnull String rsaSigningKey) throws IOException, InvalidKeyException {
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
    result.append(Hex.encodeHex(sha256.digest()));

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
    return Hex.encodeHexString(signature);
  }

  private static PrivateKey initializeRsaSigningKey(String rsaSigningKey) throws IOException, InvalidKeyException {
    final PEMReader pemReader          = new PEMReader(new StringReader(rsaSigningKey));
    final PrivateKey key               = (PrivateKey) pemReader.readObject();
    testKeyIsValidForSigning(key);
    return key;
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
