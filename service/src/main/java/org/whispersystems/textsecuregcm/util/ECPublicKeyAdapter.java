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
import java.io.IOException;
import java.util.Base64;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.ECPublicKey;

public class ECPublicKeyAdapter {

  public static class Serializer extends JsonSerializer<ECPublicKey> {

    @Override
    public void serialize(final ECPublicKey ecPublicKey,
        final JsonGenerator jsonGenerator,
        final SerializerProvider serializers) throws IOException {

      jsonGenerator.writeString(Base64.getEncoder().encodeToString(ecPublicKey.serialize()));
    }
  }

  public static class Deserializer extends JsonDeserializer<ECPublicKey> {

    @Override
    public ECPublicKey deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {
      final byte[] ecPublicKeyBytes;

      try {
        ecPublicKeyBytes = Base64.getDecoder().decode(parser.getValueAsString());
      } catch (final IllegalArgumentException e) {
        throw new JsonParseException(parser, "Could not parse EC public key as a base64-encoded value", e);
      }

      try {
        return new ECPublicKey(ecPublicKeyBytes);
      } catch (final InvalidKeyException e) {
        throw new JsonParseException(parser, "Could not interpret key bytes as an EC public key", e);
      }
    }
  }
}
