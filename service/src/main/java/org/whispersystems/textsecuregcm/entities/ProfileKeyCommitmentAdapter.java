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
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCommitment;

public class ProfileKeyCommitmentAdapter {

  public static class Serializing extends JsonSerializer<ProfileKeyCommitment> {
    @Override
    public void serialize(ProfileKeyCommitment value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      gen.writeString(Base64.getEncoder().encodeToString(value.serialize()));
    }
  }

  public static class Deserializing extends JsonDeserializer<ProfileKeyCommitment> {

    @Override
    public ProfileKeyCommitment deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
      try {
        return new ProfileKeyCommitment(Base64.getDecoder().decode(p.getValueAsString()));
      } catch (InvalidInputException e) {
        throw new IOException(e);
      }
    }
  }
}

