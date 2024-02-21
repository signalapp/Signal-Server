package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.util.Base64;
import org.signal.libsignal.protocol.InvalidKeyException;

abstract class AbstractPublicKeyDeserializer<K> extends JsonDeserializer<K> {

  @Override
  public K deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {
    final byte[] publicKeyBytes;

    try {
      publicKeyBytes = Base64.getDecoder().decode(parser.getValueAsString());
    } catch (final IllegalArgumentException e) {
      throw new JsonParseException(parser, "Could not parse public key as a base64-encoded value", e);
    }

    if (publicKeyBytes.length == 0) {
      return null;
    }

    try {
      return deserializePublicKey(publicKeyBytes);
    } catch (final InvalidKeyException e) {
      throw new JsonParseException(parser, "Could not interpret key bytes as a public key", e);
    }
  }

  protected abstract K deserializePublicKey(final byte[] publicKeyBytes) throws InvalidKeyException;
}
