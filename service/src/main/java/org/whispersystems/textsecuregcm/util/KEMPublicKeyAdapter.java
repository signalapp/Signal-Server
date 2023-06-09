/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.kem.KEMPublicKey;
import java.io.IOException;
import java.util.Base64;

public class KEMPublicKeyAdapter {

  public static class Serializer extends JsonSerializer<KEMPublicKey> {

    @Override
    public void serialize(final KEMPublicKey kemPublicKey,
        final JsonGenerator jsonGenerator,
        final SerializerProvider serializers) throws IOException {

      jsonGenerator.writeString(Base64.getEncoder().encodeToString(kemPublicKey.serialize()));
    }
  }

  public static class Deserializer extends JsonDeserializer<KEMPublicKey> {

    @Override
    public KEMPublicKey deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {
      final byte[] kemPublicKeyBytes;

      try {
        kemPublicKeyBytes = Base64.getDecoder().decode(parser.getValueAsString());
      } catch (final IllegalArgumentException e) {
        throw new JsonParseException(parser, "Could not parse KEM public key as a base64-encoded value", e);
      }

      if (kemPublicKeyBytes.length == 0) {
        return null;
      }

      try {
        return new KEMPublicKey(kemPublicKeyBytes);
      } catch (final InvalidKeyException e) {
        throw new JsonParseException(parser, "Could not interpret key bytes as a KEM public key", e);
      }
    }
  }
}
