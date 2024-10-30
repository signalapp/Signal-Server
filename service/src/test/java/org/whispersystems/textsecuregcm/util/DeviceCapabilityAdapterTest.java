/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.EnumSet;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;

class DeviceCapabilityAdapterTest {

  private record TestObject(
      @JsonSerialize(using = DeviceCapabilityAdapter.Serializer.class)
      @JsonDeserialize(using = DeviceCapabilityAdapter.Deserializer.class)
      @Nullable
      EnumSet<DeviceCapability> capabilities) {
  }

  @Test
  void serializeDeserialize() throws JsonProcessingException {
    {
      final TestObject testObject = new TestObject(EnumSet.of(DeviceCapability.TRANSFER, DeviceCapability.STORAGE));
      final String json = SystemMapper.jsonMapper().writeValueAsString(testObject);

      assertEquals(testObject, SystemMapper.jsonMapper().readValue(json, TestObject.class));
    }

    {
      final TestObject testObject = new TestObject(EnumSet.noneOf(DeviceCapability.class));
      final String json = SystemMapper.jsonMapper().writeValueAsString(testObject);

      assertEquals(testObject, SystemMapper.jsonMapper().readValue(json, TestObject.class));
    }

    {
      final TestObject testObject = new TestObject(null);
      final String json = SystemMapper.jsonMapper().writeValueAsString(testObject);

      assertEquals(testObject, SystemMapper.jsonMapper().readValue(json, TestObject.class));
    }

    {
      final String json = """
          {
            "capabilities": {
              "transfer": true,
              "unrecognizedCapability": true
            }
          }
          """;

      assertEquals(new TestObject(EnumSet.of(DeviceCapability.TRANSFER)),
          SystemMapper.jsonMapper().readValue(json, TestObject.class));
    }

    {
      final String json = """
          {
            "capabilities": {
              "transfer": true,
              "deleteSync": false
            }
          }
          """;

      assertEquals(new TestObject(EnumSet.of(DeviceCapability.TRANSFER)),
          SystemMapper.jsonMapper().readValue(json, TestObject.class));
    }
  }
}
