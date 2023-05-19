package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

import java.io.IOException;
import java.util.Base64;
import java.util.Optional;

public class OptionalBase64ByteArrayDeserializer extends JsonDeserializer<Optional<byte[]>> {

  @Override
  public Optional<byte[]> deserialize(final JsonParser jsonParser, final DeserializationContext deserializationContext) throws IOException {
    return Optional.of(Base64.getDecoder().decode(jsonParser.getValueAsString()));
  }

  @Override
  public Optional<byte[]> getNullValue(DeserializationContext ctxt) {
    return Optional.empty();
  }
}
