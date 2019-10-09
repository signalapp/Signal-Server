package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.signal.zkgroup.InvalidInputException;
import org.signal.zkgroup.profiles.ProfileKeyCommitment;
import org.whispersystems.textsecuregcm.util.Base64;

import java.io.IOException;

public class ProfileKeyCommitmentAdapter {

  public static class Serializing extends JsonSerializer<ProfileKeyCommitment> {
    @Override
    public void serialize(ProfileKeyCommitment value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      gen.writeString(Base64.encodeBytes(value.serialize()));
    }
  }

  public static class Deserializing extends JsonDeserializer<ProfileKeyCommitment> {

    @Override
    public ProfileKeyCommitment deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
      try {
        return new ProfileKeyCommitment(Base64.decode(p.getValueAsString()));
      } catch (InvalidInputException e) {
        throw new IOException(e);
      }
    }
  }
}

