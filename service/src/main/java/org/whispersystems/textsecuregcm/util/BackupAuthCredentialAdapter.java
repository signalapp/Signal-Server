/*
 * Copyright 2023 Signal Messenger, LLC
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
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.util.Base64;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.backups.BackupAuthCredentialRequest;
import org.signal.libsignal.zkgroup.internal.ByteArray;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

public class BackupAuthCredentialAdapter {

  private static final Counter INVALID_BASE64_COUNTER =
      Metrics.counter(MetricsUtil.name(BackupAuthCredentialAdapter.class, "invalidBase64"));

  private static final Counter INVALID_BYTES_COUNTER =
      Metrics.counter(MetricsUtil.name(BackupAuthCredentialAdapter.class, "invalidBackupAuthObject"));

  abstract static class GenericDeserializer<T> extends JsonDeserializer<T> {

    abstract T deserialize(final byte[] bytes) throws InvalidInputException;

    @Override
    public T deserialize(final JsonParser parser, final DeserializationContext deserializationContext)
        throws IOException {
      final byte[] bytes;
      try {
        bytes = Base64.getDecoder().decode(parser.getValueAsString());
      } catch (final IllegalArgumentException e) {
        INVALID_BASE64_COUNTER.increment();
        throw new JsonParseException(parser, "Could not parse string as a base64-encoded value", e);
      }
      try {
        return deserialize(bytes);
      } catch (InvalidInputException e) {
        INVALID_BYTES_COUNTER.increment();
        throw new JsonParseException(parser, "Could not interpret bytes as a BackupAuth object");
      }
    }
  }

  static class GenericSerializer<T extends ByteArray> extends JsonSerializer<T> {

    @Override
    public void serialize(final T t, final JsonGenerator jsonGenerator, final SerializerProvider serializerProvider)
        throws IOException {
      jsonGenerator.writeString(Base64.getEncoder().encodeToString(t.serialize()));
    }
  }

  public static class CredentialRequestSerializer extends GenericSerializer<BackupAuthCredentialRequest> {}
  public static class CredentialRequestDeserializer extends GenericDeserializer<BackupAuthCredentialRequest> {
    @Override
    BackupAuthCredentialRequest deserialize(final byte[] bytes) throws InvalidInputException {
      return new BackupAuthCredentialRequest(bytes);
    }
  }


}
