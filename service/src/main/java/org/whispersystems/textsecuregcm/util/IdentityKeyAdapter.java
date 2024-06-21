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
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.InvalidKeyException;

public class IdentityKeyAdapter {

  public static class Serializer extends JsonSerializer<IdentityKey> {

    @Override
    public void serialize(final IdentityKey identityKey,
        final JsonGenerator jsonGenerator,
        final SerializerProvider serializers) throws IOException {

      jsonGenerator.writeString(Base64.getEncoder().encodeToString(identityKey.serialize()));
    }
  }

  public static class Deserializer extends JsonDeserializer<IdentityKey> {

    @Override
    public IdentityKey deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {
      final byte[] identityKeyBytes;

      try {
        identityKeyBytes = Base64.getDecoder().decode(parser.getValueAsString());
      } catch (final IllegalArgumentException e) {
        throw new JsonParseException(parser, "Could not parse identity key as a base64-encoded value", e);
      }

      if (identityKeyBytes.length == 0) {
        return null;
      }

      try {
        return new IdentityKey(identityKeyBytes);
      } catch (final InvalidKeyException e) {
        throw new JsonParseException(parser, "Could not interpret identity key bytes as an EC public key", e);
      }
    }
  }
}
