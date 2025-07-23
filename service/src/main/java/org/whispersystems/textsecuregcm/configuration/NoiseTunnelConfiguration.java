package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import javax.annotation.Nullable;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPrivateKey;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;

public record NoiseTunnelConfiguration(@Positive int webSocketPort,
                                       @Positive int directPort,
                                       @Nullable String tlsKeyStoreFile,
                                       @Nullable String tlsKeyStoreEntryAlias,
                                       @Nullable SecretString tlsKeyStorePassword,
                                       @NotNull SecretBytes noiseStaticPrivateKey,
                                       @NotNull SecretString recognizedProxySecret) {

  public ECKeyPair noiseStaticKeyPair() throws InvalidKeyException {
    final ECPrivateKey privateKey = new ECPrivateKey(noiseStaticPrivateKey().value());

    return new ECKeyPair(privateKey.publicKey(), privateKey);
  }
}
