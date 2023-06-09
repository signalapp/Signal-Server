/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECPublicKey;

import javax.annotation.Nullable;
import java.util.Base64;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ECPublicKeyAdapterTest {

  private static final ECPublicKey EC_PUBLIC_KEY = Curve.generateKeyPair().getPublicKey();

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
    final String template = """
        {
          "publicKey": %s
        }
        """;

    return Stream.of(
        Arguments.of(String.format(template, "null"), null),
        Arguments.of(String.format(template, "\"\""), null),
        Arguments.of(String.format(template, "\"" + Base64.getEncoder().encodeToString(EC_PUBLIC_KEY.serialize()) + "\""), EC_PUBLIC_KEY),
        Arguments.of(String.format(template, "\"" + Base64.getEncoder().encodeToString(EC_PUBLIC_KEY.getPublicKeyBytes()) + "\""), EC_PUBLIC_KEY)
    );
  }
}
