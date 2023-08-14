package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import java.io.IOException;
import java.util.Base64;

/**
 * Serializes a byte array as a standard Base64-encoded string and deserializes Base64-encoded strings to byte arrays,
 * but treats any string that cannot be parsed as Base64 to {@code null}.
 * <p/>
 * Historically, device names were passed around as weakly-typed strings with the expectation that clients would provide
 * Base64 strings, but nothing in the server ever verified that was the case. In the absence of strict validation, some
 * third-party clients started submitting unencrypted names for devices, and so device names in persistent storage are a
 * mix of Base64-encoded device name ciphertexts from first-party clients and plaintext device names from third-party
 * clients. This adapter will discard the latter.
 */
public class DeviceNameByteArrayAdapter {

  private static final Counter UNPARSEABLE_DEVICE_NAME_COUNTER =
      Metrics.counter(MetricsUtil.name(DeviceNameByteArrayAdapter.class, "unparseableDeviceName"));

  public static class Serializer extends JsonSerializer<byte[]> {
    @Override
    public void serialize(final byte[] bytes,
        final JsonGenerator jsonGenerator,
        final SerializerProvider serializerProvider) throws IOException {

      jsonGenerator.writeString(Base64.getEncoder().encodeToString(bytes));
    }
  }

  public static class Deserializer extends JsonDeserializer<byte[]> {
    @Override
    public byte[] deserialize(final JsonParser jsonParser,
        final DeserializationContext deserializationContext) throws IOException {

      try {
        return Base64.getDecoder().decode(jsonParser.getValueAsString());
      } catch (final IllegalArgumentException e) {
        UNPARSEABLE_DEVICE_NAME_COUNTER.increment();
        return null;
      }
    }
  }
}
