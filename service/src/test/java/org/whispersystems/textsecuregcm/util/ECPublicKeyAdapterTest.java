/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Base64;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.protocol.ecc.ECPublicKey;

class ECPublicKeyAdapterTest {

  private static final String JSON_TEMPLATE = """
      {
        "publicKey": %s
      }
      """;

  private static final ECPublicKey EC_PUBLIC_KEY = ECKeyPair.generate().getPublicKey();

  private record ECPublicKeyCarrier(@JsonSerialize(using = ECPublicKeyAdapter.Serializer.class)
                                    @JsonDeserialize(using = ECPublicKeyAdapter.Deserializer.class)
                                    ECPublicKey publicKey) {
  }

  @ParameterizedTest
  @MethodSource
  void deserialize(final String json, @Nullable final ECPublicKey expectedPublicKey) throws JsonProcessingException {
    final ECPublicKeyCarrier publicKeyCarrier = SystemMapper.jsonMapper().readValue(json, ECPublicKeyCarrier.class);

    assertEquals(expectedPublicKey, publicKeyCarrier.publicKey());
  }

  private static Stream<Arguments> deserialize() {
    return Stream.of(
        Arguments.of(String.format(JSON_TEMPLATE, "null"), null),
        Arguments.of(String.format(JSON_TEMPLATE, "\"\""), null),
        Arguments.of(String.format(JSON_TEMPLATE, "\"" + Base64.getEncoder().encodeToString(EC_PUBLIC_KEY.serialize()) + "\""), EC_PUBLIC_KEY)
    );
  }

  @ParameterizedTest
  @MethodSource
  void deserializeInvalidKey(final String json) {
    assertThrows(JsonMappingException.class,
        () -> SystemMapper.jsonMapper().readValue(json, ECPublicKeyCarrier.class));
  }

  private static Stream<String> deserializeInvalidKey() {
    return Stream.of(
        String.format(JSON_TEMPLATE, "\"" + Base64.getEncoder().encodeToString(new byte[12]) + "\""),
        String.format(JSON_TEMPLATE, "\"This is not a legal base64-encoded string\"")
    );
  }
}
