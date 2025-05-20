/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.signal.libsignal.protocol.InvalidKeyException;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.entities.KEMSignedPreKey;
import org.whispersystems.textsecuregcm.tests.util.KeysHelper;

class KEMPreKeyPageTest {

  private static final ECKeyPair IDENTITY_KEY_PAIR = Curve.generateKeyPair();

  @Test
  void serializeSinglePreKey() {
    final ByteBuffer page = KEMPreKeyPage.serialize(KEMPreKeyPage.FORMAT, List.of(generatePreKey(5)));
    final int actualMagic = page.getInt();
    assertEquals(KEMPreKeyPage.HEADER_MAGIC, actualMagic);
    final int version = page.getInt();
    assertEquals(version, 1);
    assertEquals(KEMPreKeyPage.SERIALIZED_PREKEY_LENGTH, page.remaining());
  }

  @Test
  void emptyPreKeys() {
    assertThrows(IllegalArgumentException.class, () -> KEMPreKeyPage.serialize(KEMPreKeyPage.FORMAT, Collections.emptyList()));
  }

  @Test
  void roundTripSingleton() throws InvalidKeyException {
    final KEMSignedPreKey preKey = generatePreKey(5);
    final ByteBuffer buffer = KEMPreKeyPage.serialize(KEMPreKeyPage.FORMAT, List.of(preKey));
    final long serializedLength = buffer.remaining();
    assertEquals(KEMPreKeyPage.HEADER_SIZE + KEMPreKeyPage.SERIALIZED_PREKEY_LENGTH, serializedLength);

    final KEMPreKeyPage.KeyLocation keyLocation = KEMPreKeyPage.keyLocation(1, 0);
    assertEquals(KEMPreKeyPage.HEADER_SIZE, keyLocation.getStartInclusive());
    assertEquals(serializedLength, KEMPreKeyPage.HEADER_SIZE + keyLocation.length());

    buffer.position(keyLocation.getStartInclusive());
    final KEMSignedPreKey deserializedPreKey = KEMPreKeyPage.deserializeKey(1, buffer);

    assertEquals(5L, deserializedPreKey.keyId());
    assertEquals(preKey, deserializedPreKey);
  }

  @Test
  void roundTripMultiple() throws InvalidKeyException {
    final List<KEMSignedPreKey> keys = Arrays.asList(generatePreKey(1), generatePreKey(2), generatePreKey(5));
    final ByteBuffer page = KEMPreKeyPage.serialize(KEMPreKeyPage.FORMAT, keys);

    assertEquals(KEMPreKeyPage.HEADER_SIZE + KEMPreKeyPage.SERIALIZED_PREKEY_LENGTH * 3, page.remaining());

    for (int i = 0; i < keys.size(); i++) {
      final KEMPreKeyPage.KeyLocation keyLocation = KEMPreKeyPage.keyLocation(1, i);
      assertEquals(
          KEMPreKeyPage.HEADER_SIZE + KEMPreKeyPage.SERIALIZED_PREKEY_LENGTH * i,
          keyLocation.getStartInclusive());
      final ByteBuffer buf = page.slice(keyLocation.getStartInclusive(), keyLocation.length());
      final KEMSignedPreKey actual = KEMPreKeyPage.deserializeKey(1, buf);
      assertEquals(keys.get(i), actual);
    }
  }

  @Test
  void wrongFormat() {
    assertThrows(IllegalArgumentException.class, () ->
        KEMPreKeyPage.deserializeKey(2,
            ByteBuffer.allocate(KEMPreKeyPage.HEADER_SIZE + KEMPreKeyPage.SERIALIZED_PREKEY_LENGTH)));
  }

  @Test
  void wrongSize() {
    assertThrows(IllegalArgumentException.class, () -> KEMPreKeyPage.deserializeKey(1, ByteBuffer.allocate(100)));
  }


  @Test
  void negativeKeyId() throws InvalidKeyException {
    final KEMSignedPreKey preKey = generatePreKey(-1);
    ByteBuffer page = KEMPreKeyPage.serialize(KEMPreKeyPage.FORMAT, List.of(preKey));
    page.position(KEMPreKeyPage.HEADER_SIZE);
    KEMSignedPreKey deserializedPreKey = KEMPreKeyPage.deserializeKey(1, page);
    assertEquals(-1L, deserializedPreKey.keyId());
  }

  private static KEMSignedPreKey generatePreKey(long keyId) {
    return KeysHelper.signedKEMPreKey((int) keyId, IDENTITY_KEY_PAIR);
  }

}
