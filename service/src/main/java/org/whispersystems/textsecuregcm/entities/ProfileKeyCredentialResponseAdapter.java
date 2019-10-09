package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.signal.zkgroup.InvalidInputException;
import org.signal.zkgroup.profiles.ProfileKeyCredentialResponse;
import org.whispersystems.textsecuregcm.util.Base64;

import java.io.IOException;

public class ProfileKeyCredentialResponseAdapter {

  public static class Serializing extends JsonSerializer<ProfileKeyCredentialResponse> {
    @Override
    public void serialize(ProfileKeyCredentialResponse response, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException, JsonProcessingException
    {
      if (response == null) jsonGenerator.writeNull();
      else                  jsonGenerator.writeString(Base64.encodeBytes(response.serialize()));
    }
  }

  public static class Deserializing extends JsonDeserializer<ProfileKeyCredentialResponse> {
    @Override
    public ProfileKeyCredentialResponse deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException, JsonProcessingException
    {
      try {
        return new ProfileKeyCredentialResponse(Base64.decode(jsonParser.getValueAsString()));
      } catch (InvalidInputException e) {
        throw new IOException(e);
      }
    }
  }
}
