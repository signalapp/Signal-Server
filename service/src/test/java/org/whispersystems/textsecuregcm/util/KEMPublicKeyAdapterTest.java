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
import org.signal.libsignal.protocol.kem.KEMKeyPair;
import org.signal.libsignal.protocol.kem.KEMKeyType;
import org.signal.libsignal.protocol.kem.KEMPublicKey;
import javax.annotation.Nullable;
import java.util.Base64;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class KEMPublicKeyAdapterTest {

  private static final KEMPublicKey KEM_PUBLIC_KEY = KEMKeyPair.generate(KEMKeyType.KYBER_1024).getPublicKey();

  private record KEMPublicKeyCarrier(@JsonSerialize(using = KEMPublicKeyAdapter.Serializer.class)
                                    @JsonDeserialize(using = KEMPublicKeyAdapter.Deserializer.class)
                                    KEMPublicKey publicKey) {
  }

  @ParameterizedTest
  @MethodSource
  void deserialize(final String json, @Nullable final KEMPublicKey expectedPublicKey) throws JsonProcessingException {
    final KEMPublicKeyCarrier publicKeyCarrier = SystemMapper.jsonMapper().readValue(json, KEMPublicKeyAdapterTest.KEMPublicKeyCarrier.class);

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
        Arguments.of(String.format(template, "\"" + Base64.getEncoder().encodeToString(KEM_PUBLIC_KEY.serialize()) + "\""),
            KEM_PUBLIC_KEY)
    );
  }
}
