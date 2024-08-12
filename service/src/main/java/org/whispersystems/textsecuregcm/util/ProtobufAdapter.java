/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

import java.io.IOException;
import java.util.function.Supplier;

public class ProtobufAdapter {

  public static class Serializer<T extends Message> extends JsonSerializer<T> {

    @Override
    public void serialize(T message, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      jsonGenerator.writeString(JsonFormat.printer().print(message));
    }
  }

  public static class Deserializer<T extends Message> extends JsonDeserializer<T> {

    private final Supplier<Message.Builder> builderSupplier;

    public Deserializer(Supplier<Message.Builder> builderSupplier) {
      this.builderSupplier = builderSupplier;
    }

    @Override
    public T deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
      Message.Builder builder = builderSupplier.get();
      JsonFormat.parser().ignoringUnknownFields().merge(jsonParser.getValueAsString(), builder);
      return (T) builder.build();
    }
  }
}
