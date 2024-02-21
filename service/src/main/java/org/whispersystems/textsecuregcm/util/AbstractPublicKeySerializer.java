package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.Base64;

abstract class AbstractPublicKeySerializer<K> extends JsonSerializer<K> {

  @Override
  public void serialize(final K publicKey,
      final JsonGenerator jsonGenerator,
      final SerializerProvider serializerProvider) throws IOException {

    jsonGenerator.writeString(Base64.getEncoder().encodeToString(serializePublicKey(publicKey)));
  }

  protected abstract byte[] serializePublicKey(final K publicKey);
}
