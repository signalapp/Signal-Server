/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECParameterSpec;
import java.security.spec.ECPoint;
import java.security.spec.ECPublicKeySpec;
import java.util.Base64;

import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.crypto.tink.subtle.EllipticCurves;

import io.micrometer.core.instrument.Metrics;

public class P256ECPublicKeyAdapter {

  private static final String REASON_TAG_NAME = "reason";

  public static class Serializer extends JsonSerializer<ECPublicKey> {
    private final String invalidKeyCounterName = MetricsUtil.name(getClass(), "invalidKey");

    @Override
    public void serialize(final ECPublicKey publicKey,
        final JsonGenerator jsonGenerator,
        final SerializerProvider serializerProvider) throws IOException {
      try {
      jsonGenerator.writeString(Base64.getUrlEncoder().encodeToString(serializePublicKey(publicKey)));
      } catch (final GeneralSecurityException e) {
        Metrics.counter(invalidKeyCounterName, REASON_TAG_NAME, "invalid-key").increment();
        throw new JsonGenerationException("Could not serialize public key", e, jsonGenerator);
      }

    }

    private byte[] serializePublicKey(final ECPublicKey publicKey) throws GeneralSecurityException {
      return EllipticCurves.pointEncode(
        EllipticCurves.CurveType.NIST_P256,
        EllipticCurves.PointFormatType.UNCOMPRESSED,
        publicKey.getW()
      );
    }
  }

  public static class Deserializer extends JsonDeserializer<ECPublicKey> {
    private final String invalidKeyCounterName = MetricsUtil.name(getClass(), "invalidKey");

    @Override
    public ECPublicKey deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {
      final byte[] publicKeyBytes;

      try {
        publicKeyBytes = Base64.getUrlDecoder().decode(parser.getValueAsString());
      } catch (final IllegalArgumentException e) {
        Metrics.counter(invalidKeyCounterName, REASON_TAG_NAME, "illegal-base64").increment();
        throw new JsonParseException(parser, "Could not parse public key as a base64-encoded value", e);
      }

      try {
        return deserializePublicKey(publicKeyBytes);
      } catch (final GeneralSecurityException e) {
        Metrics.counter(invalidKeyCounterName, REASON_TAG_NAME, "invalid-key").increment();
        throw new JsonParseException(parser, "Could not interpret key bytes as a public key", e);
      }
    }

    private static ECPublicKey deserializePublicKey(final byte[] publicKeyBytes) throws GeneralSecurityException {
      ECPoint point = EllipticCurves.pointDecode(
        EllipticCurves.CurveType.NIST_P256,
        EllipticCurves.PointFormatType.UNCOMPRESSED,
        publicKeyBytes
      );
      ECParameterSpec spec = EllipticCurves.getCurveSpec(EllipticCurves.CurveType.NIST_P256);
      KeyFactory factory = KeyFactory.getInstance("EC");
      return (ECPublicKey) factory.generatePublic(new ECPublicKeySpec(point, spec));
    }

    public static ECPublicKey deserializePublicKey(final String b64PublicKey) throws GeneralSecurityException {
      byte[] publicKeyBytes = Base64.getUrlDecoder().decode(b64PublicKey);
      return deserializePublicKey(publicKeyBytes);
    }
  }
}
