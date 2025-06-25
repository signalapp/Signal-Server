/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

public class FoundationDbTest {

  @RegisterExtension
  static FoundationDbExtension FOUNDATION_DB_EXTENSION = new FoundationDbExtension();

  @Test
  void setGetValue() {
    final byte[] key = "test".getBytes(StandardCharsets.UTF_8);
    final byte[] value = TestRandomUtil.nextBytes(16);

    FOUNDATION_DB_EXTENSION.getDatabase().run(transaction -> {
      transaction.set(key, value);
      return null;
    });

    final byte[] retrievedValue = FOUNDATION_DB_EXTENSION.getDatabase().run(transaction -> transaction.get(key).join());

    assertArrayEquals(value, retrievedValue);
  }
}
