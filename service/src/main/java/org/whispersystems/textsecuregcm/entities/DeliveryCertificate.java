package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import org.whispersystems.textsecuregcm.util.Base64;
import org.whispersystems.textsecuregcm.util.ByteArrayAdapter;

import java.io.IOException;

public class DeliveryCertificate {

  @JsonProperty
  @JsonSerialize(using = ByteArraySerializer.class)
  @JsonDeserialize(using = ByteArrayDeserializer.class)
  private byte[] certificate;

  public DeliveryCertificate(byte[] certificate) {
    this.certificate = certificate;
  }

  public DeliveryCertificate() {}

  @VisibleForTesting
  public byte[] getCertificate() {
    return certificate;
  }

  public static class ByteArraySerializer extends JsonSerializer<byte[]> {
    @Override
    public void serialize(byte[] bytes, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {
      jsonGenerator.writeString(Base64.encodeBytes(bytes));
    }
  }

  public static class ByteArrayDeserializer extends JsonDeserializer<byte[]> {
    @Override
    public byte[] deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      return Base64.decode(jsonParser.getValueAsString());
    }
  }
}
