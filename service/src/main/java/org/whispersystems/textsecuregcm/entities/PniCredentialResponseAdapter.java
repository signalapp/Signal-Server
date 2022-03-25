/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.Base64;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.profiles.PniCredentialResponse;

public class PniCredentialResponseAdapter {

  public static class Serializing extends JsonSerializer<PniCredentialResponse> {
    @Override
    public void serialize(PniCredentialResponse response, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      if (response == null) jsonGenerator.writeNull();
      else                  jsonGenerator.writeString(Base64.getEncoder().encodeToString(response.serialize()));
    }
  }

  public static class Deserializing extends JsonDeserializer<PniCredentialResponse> {
    @Override
    public PniCredentialResponse deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException {
      try {
        return new PniCredentialResponse(Base64.getDecoder().decode(jsonParser.getValueAsString()));
      } catch (InvalidInputException e) {
        throw new IOException(e);
      }
    }
  }
}
