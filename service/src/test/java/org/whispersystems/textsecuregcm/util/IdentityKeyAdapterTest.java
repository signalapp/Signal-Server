/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.Base64;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;

class IdentityKeyAdapterTest {

  private static final IdentityKey IDENTITY_KEY = new IdentityKey(ECKeyPair.generate().getPublicKey());

  private record IdentityKeyCarrier(@JsonSerialize(using = IdentityKeyAdapter.Serializer.class)
                                    @JsonDeserialize(using = IdentityKeyAdapter.Deserializer.class)
                                    IdentityKey identityKey) {

  };

  @ParameterizedTest
  @MethodSource
  void deserialize(final String json, @Nullable final IdentityKey expectedIdentityKey) throws JsonProcessingException {
    final IdentityKeyCarrier identityKeyCarrier = SystemMapper.jsonMapper().readValue(json, IdentityKeyCarrier.class);

    assertEquals(expectedIdentityKey, identityKeyCarrier.identityKey());
  }

  private static Stream<Arguments> deserialize() {
    final String template = """
        {
          "identityKey": %s
        }
        """;

    return Stream.of(
        Arguments.of(String.format(template, "null"), null),
        Arguments.of(String.format(template, "\"\""), null),
        Arguments.of(
            String.format(template, "\"" + Base64.getEncoder().encodeToString(IDENTITY_KEY.serialize()) + "\""),
            IDENTITY_KEY)
    );
  }
}
