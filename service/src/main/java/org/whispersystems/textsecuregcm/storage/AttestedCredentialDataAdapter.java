/**
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.io.IOException;
import java.util.Base64;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.webauthn4j.converter.AttestedCredentialDataConverter;
import com.webauthn4j.converter.util.ObjectConverter;
import com.webauthn4j.data.attestation.authenticator.AttestedCredentialData;

public class AttestedCredentialDataAdapter {

  private static final ObjectConverter OBJECT_CONVERTER = new ObjectConverter();
  private static final AttestedCredentialDataConverter ATTESTED_CREDENTIAL_DATA_CONVERTER =
      new AttestedCredentialDataConverter(OBJECT_CONVERTER);

  public static class Serializer extends JsonSerializer<AttestedCredentialData> {

    @Override
    public void serialize(final AttestedCredentialData attestedCredentialData, final JsonGenerator jsonGenerator,
        final SerializerProvider serializerProvider)
        throws IOException {

      jsonGenerator.writeString(
          Base64.getEncoder()
              .withoutPadding()
              .encodeToString(ATTESTED_CREDENTIAL_DATA_CONVERTER.convert(attestedCredentialData)));
    }
  }

  public static class Deserializer extends JsonDeserializer<AttestedCredentialData> {

    @Override
    public AttestedCredentialData deserialize(final JsonParser jsonParser,
        final DeserializationContext deserializationContext) throws IOException {

      return ATTESTED_CREDENTIAL_DATA_CONVERTER.convert(Base64.getDecoder().decode(jsonParser.getValueAsString()));
    }
  }
}
