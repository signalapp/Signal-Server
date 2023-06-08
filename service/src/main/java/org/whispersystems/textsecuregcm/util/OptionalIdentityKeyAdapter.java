/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.Base64;
import java.util.Optional;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.InvalidKeyException;

public class OptionalIdentityKeyAdapter {

  public static class Serializer extends JsonSerializer<Optional<IdentityKey>> {

    @Override
    public void serialize(final Optional<IdentityKey> maybePublicKey,
        final JsonGenerator jsonGenerator,
        final SerializerProvider serializers) throws IOException {

      if (maybePublicKey.isPresent()) {
        jsonGenerator.writeString(Base64.getEncoder().encodeToString(maybePublicKey.get().serialize()));
      } else {
        jsonGenerator.writeNull();
      }
    }
  }

  public static class Deserializer extends JsonDeserializer<Optional<IdentityKey>> {

    @Override
    public Optional<IdentityKey> deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
      try {
        return Optional.of(new IdentityKey(Base64.getDecoder().decode(jsonParser.getValueAsString())));
      } catch (final InvalidKeyException e) {
        throw new IOException(e);
      }
    }

    @Override
    public Optional<IdentityKey> getNullValue(DeserializationContext ctxt) {
      return Optional.empty();
    }
  }
}
