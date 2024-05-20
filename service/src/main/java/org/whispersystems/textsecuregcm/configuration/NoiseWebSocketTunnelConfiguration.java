package org.whispersystems.textsecuregcm.configuration;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPrivateKey;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;

public record NoiseWebSocketTunnelConfiguration(@Positive int port,
                                                @Nullable String tlsKeyStoreFile,
                                                @Nullable String tlsKeyStoreEntryAlias,
                                                @Nullable SecretString tlsKeyStorePassword,
                                                @NotNull SecretBytes noiseStaticPrivateKey,
                                                @NotNull byte[] noiseRootPublicKeySignature,
                                                @NotNull SecretString recognizedProxySecret) {

  public ECKeyPair noiseStaticKeyPair() throws InvalidKeyException {
    final ECPrivateKey privateKey = Curve.decodePrivatePoint(noiseStaticPrivateKey().value());

    return new ECKeyPair(privateKey.publicKey(), privateKey);
  }
}
