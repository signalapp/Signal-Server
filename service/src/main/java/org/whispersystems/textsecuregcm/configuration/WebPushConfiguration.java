package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.constraints.NotNull;

import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECPoint;
import java.security.spec.ECPublicKeySpec;
import java.security.spec.PKCS8EncodedKeySpec;

import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;

import com.google.crypto.tink.internal.EllipticCurvesUtil;

public record WebPushConfiguration(@NotNull SecretBytes vapidStaticPrivateKey, KeyPair keyPair) {
  public WebPushConfiguration(@NotNull SecretBytes vapidStaticPrivateKey) {
    this(vapidStaticPrivateKey, null);
  }

  public WebPushConfiguration {
    try {
      keyPair = genVapidKeyPair(vapidStaticPrivateKey);
    } catch (GeneralSecurityException e) {}
  }

  private static KeyPair genVapidKeyPair(SecretBytes vapidStaticPrivateKey) throws GeneralSecurityException {
    final KeyFactory kf = KeyFactory.getInstance("EC");
    final ECPrivateKey privkey = (ECPrivateKey) kf.generatePrivate(
      new PKCS8EncodedKeySpec(vapidStaticPrivateKey.value())
    );
    final ECPoint w = EllipticCurvesUtil.multiplyByGenerator(privkey.getS(), privkey.getParams());
    final ECPublicKeySpec spec = new ECPublicKeySpec(w, privkey.getParams());
    final ECPublicKey pubkey = (ECPublicKey) kf.generatePublic(spec);
    return new KeyPair(pubkey, privkey);
  }

  public KeyPair vapidStaticKeyPair() {
    return keyPair;
  }
}
