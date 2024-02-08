/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.identity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

class ServiceIdentifierTest {
  
  @ParameterizedTest
  @MethodSource
  void valueOf(final String identifierString, final IdentityType expectedIdentityType, final UUID expectedUuid) {
    final ServiceIdentifier serviceIdentifier = ServiceIdentifier.valueOf(identifierString);

    assertEquals(expectedIdentityType, serviceIdentifier.identityType());
    assertEquals(expectedUuid, serviceIdentifier.uuid());
  }

  private static Stream<Arguments> valueOf() {
    final UUID uuid = UUID.randomUUID();

    return Stream.of(
        Arguments.of(uuid.toString(), IdentityType.ACI, uuid),
        Arguments.of("PNI:" + uuid, IdentityType.PNI, uuid));
  }

  @ParameterizedTest
  @ValueSource(strings = {"Not a valid UUID", "BAD:a9edc243-3e93-45d4-95c6-e3a84cd4a254", "ACI:a9edc243-3e93-45d4-95c6-e3a84cd4a254"})
  void valueOfIllegalArgument(final String identifierString) {
    assertThrows(IllegalArgumentException.class, () -> ServiceIdentifier.valueOf(identifierString));
  }

  @ParameterizedTest
  @MethodSource
  void fromBytes(final byte[] bytes, final IdentityType expectedIdentityType, final UUID expectedUuid) {
    final ServiceIdentifier serviceIdentifier = ServiceIdentifier.fromBytes(bytes);

    assertEquals(expectedIdentityType, serviceIdentifier.identityType());
    assertEquals(expectedUuid, serviceIdentifier.uuid());
  }

  private static Stream<Arguments> fromBytes() {
    final UUID uuid = UUID.randomUUID();

    final byte[] aciPrefixedBytes = new byte[17];
    aciPrefixedBytes[0] = 0x00;
    System.arraycopy(UUIDUtil.toBytes(uuid), 0, aciPrefixedBytes, 1, 16);

    final byte[] pniPrefixedBytes = new byte[17];
    pniPrefixedBytes[0] = 0x01;
    System.arraycopy(UUIDUtil.toBytes(uuid), 0, pniPrefixedBytes, 1, 16);

    return Stream.of(
        Arguments.of(UUIDUtil.toBytes(uuid), IdentityType.ACI, uuid),
        Arguments.of(aciPrefixedBytes, IdentityType.ACI, uuid),
        Arguments.of(pniPrefixedBytes, IdentityType.PNI, uuid));
  }

  @ParameterizedTest
  @MethodSource
  void fromBytesIllegalArgument(final byte[] bytes) {
    assertThrows(IllegalArgumentException.class, () -> ServiceIdentifier.fromBytes(bytes));
  }

  private static Stream<Arguments> fromBytesIllegalArgument() {
    final byte[] invalidPrefixBytes = new byte[17];
    invalidPrefixBytes[0] = (byte) 0xff;

    return Stream.of(
        Arguments.of(new byte[0]),
        Arguments.of(new byte[15]),
        Arguments.of(new byte[18]),
        Arguments.of(invalidPrefixBytes));
  }
}
