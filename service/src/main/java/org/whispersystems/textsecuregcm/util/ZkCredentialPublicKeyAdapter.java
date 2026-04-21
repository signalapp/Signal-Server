/*
 * Copyright 2026 Signal Messenger, LLC
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
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ZkCredentialPublicKey;

public class ZkCredentialPublicKeyAdapter {
  public static class Serializer extends JsonSerializer<ZkCredentialPublicKey> {

    @Override
    public void serialize(final ZkCredentialPublicKey zkCredentialPublicKey,
        final JsonGenerator jsonGenerator,
        final SerializerProvider serializers) throws IOException {

      jsonGenerator.writeString(Base64.getEncoder().encodeToString(zkCredentialPublicKey.serialize()));
    }
  }

  public static class Deserializer extends JsonDeserializer<ZkCredentialPublicKey> {

    @Override
    public ZkCredentialPublicKey deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {
      final byte[] zkCredentialPublicKeyBytes;

      try {
        zkCredentialPublicKeyBytes = Base64.getDecoder().decode(parser.getValueAsString());
      } catch (final IllegalArgumentException e) {
        throw new JsonParseException(parser, "Could not parse key as a base64-encoded value", e);
      }

      if (zkCredentialPublicKeyBytes.length == 0) {
        return null;
      }

      try {
        return new ZkCredentialPublicKey(zkCredentialPublicKeyBytes);
      } catch (final InvalidInputException e) {
        // this should really never happen, as ZkCredentialPublicKey simply extends ByteArray
        throw new JsonParseException(parser, "Could not interpret bytes as a ZK credential public key", e);
      }
    }
  }
}
