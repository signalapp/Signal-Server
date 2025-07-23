/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.integration;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.Base64;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECPublicKey;

public final class Codecs {

  private Codecs() {
    // utility class
  }

  @FunctionalInterface
  public interface CheckedFunction<T, R> {
    R apply(T t) throws Exception;
  }

  public static class Base64BasedSerializer<T> extends JsonSerializer<T> {

    private final CheckedFunction<T, byte[]> mapper;

    public Base64BasedSerializer(final CheckedFunction<T, byte[]> mapper) {
      this.mapper = mapper;
    }

    @Override
    public void serialize(final T value, final JsonGenerator gen, final SerializerProvider serializers) throws IOException {
      try {
        gen.writeString(Base64.getEncoder().withoutPadding().encodeToString(mapper.apply(value)));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class Base64BasedDeserializer<T> extends JsonDeserializer<T> {

    private final CheckedFunction<byte[], T> mapper;

    public Base64BasedDeserializer(final CheckedFunction<byte[], T> mapper) {
      this.mapper = mapper;
    }

    @Override
    public T deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException {
      try {
        return mapper.apply(Base64.getDecoder().decode(p.getValueAsString()));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  public static class ByteArraySerializer extends Base64BasedSerializer<byte[]> {
    public ByteArraySerializer() {
      super(bytes -> bytes);
    }
  }

  public static class ByteArrayDeserializer extends Base64BasedDeserializer<byte[]> {
    public ByteArrayDeserializer() {
      super(bytes -> bytes);
    }
  }

  public static class ECPublicKeySerializer extends Base64BasedSerializer<ECPublicKey> {
    public ECPublicKeySerializer() {
      super(ECPublicKey::serialize);
    }
  }

  public static class ECPublicKeyDeserializer extends Base64BasedDeserializer<ECPublicKey> {
    public ECPublicKeyDeserializer() {
      super(ECPublicKey::new);
    }
  }

  public static class IdentityKeySerializer extends Base64BasedSerializer<IdentityKey> {
    public IdentityKeySerializer() {
      super(IdentityKey::serialize);
    }
  }

  public static class IdentityKeyDeserializer extends Base64BasedDeserializer<IdentityKey> {
    public IdentityKeyDeserializer() {
      super(bytes -> new IdentityKey(bytes, 0));
    }
  }
}
