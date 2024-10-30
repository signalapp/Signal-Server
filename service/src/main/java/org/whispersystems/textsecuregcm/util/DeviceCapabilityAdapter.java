/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class DeviceCapabilityAdapter {

  private static final TypeReference<Map<String, Boolean>> STRING_TO_BOOLEAN_MAP_TYPE = new TypeReference<>() {};

  private DeviceCapabilityAdapter() {
  }

  public static class Serializer extends JsonSerializer<Set<DeviceCapability>> {

    @Override
    public void serialize(final Set<DeviceCapability> capabilities,
        final JsonGenerator jsonGenerator,
        final SerializerProvider serializerProvider) throws IOException {

      jsonGenerator.writeObject(capabilities.stream()
          .collect(Collectors.toMap(DeviceCapability::getName, ignored -> true)));
    }
  }

  public static class Deserializer extends JsonDeserializer<Set<DeviceCapability>> {

    @Override
    public Set<DeviceCapability> deserialize(final JsonParser jsonParser,
        final DeserializationContext deserializationContext) throws IOException {

      return mapToSet(jsonParser.readValueAs(STRING_TO_BOOLEAN_MAP_TYPE));
    }

  }

  public static Set<DeviceCapability> mapToSet(Map<String, Boolean> capabilitiesMap) {
    return capabilitiesMap.entrySet()
        .stream()
        .filter(Map.Entry::getValue)
        .map(entry -> DeviceCapability.forName(entry.getKey()))
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toCollection(() -> EnumSet.noneOf(DeviceCapability.class)));
  }

}
