package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import java.io.IOException;
import java.util.Base64;
import io.micrometer.core.instrument.Metrics;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

abstract class AbstractPublicKeyDeserializer<K> extends JsonDeserializer<K> {

  private final String invalidKeyCounterName = MetricsUtil.name(getClass(), "invalidKey");

  private static final String REASON_TAG_NAME = "reason";

  @Override
  public K deserialize(final JsonParser parser, final DeserializationContext context) throws IOException {
    final byte[] publicKeyBytes;

    try {
      publicKeyBytes = Base64.getDecoder().decode(parser.getValueAsString());
    } catch (final IllegalArgumentException e) {
      Metrics.counter(invalidKeyCounterName, REASON_TAG_NAME, "illegal-base64").increment();
      throw new JsonParseException(parser, "Could not parse public key as a base64-encoded value", e);
    }

    if (publicKeyBytes.length == 0) {
      return null;
    }

    try {
      return deserializePublicKey(publicKeyBytes);
    } catch (final InvalidKeyException e) {
      Metrics.counter(invalidKeyCounterName, REASON_TAG_NAME, "invalid-key").increment();
      throw new JsonParseException(parser, "Could not interpret key bytes as a public key", e);
    }
  }

  protected abstract K deserializePublicKey(final byte[] publicKeyBytes) throws InvalidKeyException;
}
