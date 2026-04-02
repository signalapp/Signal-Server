/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage.foundationdb;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.apple.foundationdb.tuple.Versionstamp;
import java.security.SecureRandom;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

class VersionstampUUIDCipherTest {

  private byte keyId;

  private VersionstampUUIDCipher versionstampUUIDCipher;

  @BeforeEach
  void setUp() {
    final byte[] keyBytes = new byte[32];
    new SecureRandom().nextBytes(keyBytes);

    keyId = (byte) (ThreadLocalRandom.current().nextInt() & 0x3f);

    versionstampUUIDCipher = new VersionstampUUIDCipher(keyId, keyBytes);
  }

  @RepeatedTest(value = 1024, failureThreshold = 1)
  void encryptDecryptVersionstamp() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = (byte) (ThreadLocalRandom.current().nextInt() & 0xff);
    final Versionstamp versionstamp = Versionstamp.fromBytes(TestRandomUtil.nextBytes(12));

    final UUID ciphertext = versionstampUUIDCipher.encryptVersionstamp(versionstamp, accountIdentifier, deviceId);
    final Versionstamp plaintext = versionstampUUIDCipher.decryptVersionstamp(ciphertext, accountIdentifier, deviceId);

    assertEquals(8, ciphertext.version());
    assertEquals(2, ciphertext.variant());
    assertEquals(keyId, VersionstampUUIDCipher.getKeyId(ciphertext));
    assertEquals(VersionstampUUIDCipher.FORMAT_VERSION, VersionstampUUIDCipher.getFormatVersion(ciphertext));
    assertEquals(versionstamp, plaintext);
  }

  @Test
  void decryptInvalidUuid() {
    assertThrows(IllegalArgumentException.class,
        () -> versionstampUUIDCipher.decryptVersionstamp(UUID.randomUUID(), UUID.randomUUID(), Device.PRIMARY_ID));
  }

  @RepeatedTest(value = 1024, failureThreshold = 1)
  void byteArrayToFromLongCarriers() {
    final byte[] versionstamp = TestRandomUtil.nextBytes(12);
    final long[] longCarriers = VersionstampUUIDCipher.byteArrayToLongs(versionstamp);

    assertEquals(2, longCarriers.length);
    assertArrayEquals(versionstamp, VersionstampUUIDCipher.longsToByteArray(longCarriers[0], longCarriers[1]));
  }

  @Test
  void getFormatVersionInvalidUuid() {
    assertThrows(IllegalArgumentException.class, () -> VersionstampUUIDCipher.getFormatVersion(UUID.randomUUID()));
  }

  @Test
  void getKeyIdInvalidUuid() {
    assertThrows(IllegalArgumentException.class, () -> VersionstampUUIDCipher.getKeyId(UUID.randomUUID()));
  }
}
