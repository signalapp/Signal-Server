package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.SerializerProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Base64;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DeviceNameByteArrayAdapterTest {

  @Test
  void serialize() throws IOException {
    final byte[] deviceName = TestRandomUtil.nextBytes(16);
    final JsonGenerator jsonGenerator = mock(JsonGenerator.class);

    new DeviceNameByteArrayAdapter.Serializer().serialize(deviceName, jsonGenerator, mock(SerializerProvider.class));

    verify(jsonGenerator).writeString(Base64.getEncoder().encodeToString(deviceName));
  }

  @ParameterizedTest
  @MethodSource
  void deserialize(final String encodedString, final byte[] expectedBytes) throws IOException {
    final JsonParser jsonParser = mock(JsonParser.class);
    when(jsonParser.getValueAsString()).thenReturn(encodedString);

    assertArrayEquals(expectedBytes,
        new DeviceNameByteArrayAdapter.Deserializer().deserialize(jsonParser, mock(DeserializationContext.class)));
  }

  private static List<Arguments> deserialize() {
    final byte[] deviceName = TestRandomUtil.nextBytes(16);

    return List.of(
        Arguments.of(Base64.getEncoder().encodeToString(deviceName), deviceName),
        Arguments.of("This is not a valid Base64 string", null)
    );
  }
}
