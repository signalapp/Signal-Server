/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.dispatch.redis.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class StringReplyHeaderTest {

  @Test
  void testNull() {
    assertThrows(IOException.class, () -> new StringReplyHeader(null));
  }

  @Test
  void testBadNumber() {
    assertThrows(IOException.class, () -> new StringReplyHeader("$100A"));
  }

  @Test
  void testBadPrefix() {
    assertThrows(IOException.class, () -> new StringReplyHeader("*"));
  }

  @Test
  void testValid() throws IOException {
    assertEquals(1000, new StringReplyHeader("$1000").getStringLength());
  }

}
