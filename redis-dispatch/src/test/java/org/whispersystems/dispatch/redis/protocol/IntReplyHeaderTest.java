/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.dispatch.redis.protocol;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class IntReplyHeaderTest {

  @Test
  void testNull() {
    assertThrows(IOException.class, () -> new IntReply(null));
  }

  @Test
  void testEmpty() {
    assertThrows(IOException.class, () -> new IntReply(""));
  }

  @Test
  void testBadNumber() {
    assertThrows(IOException.class, () -> new IntReply(":A"));
  }

  @Test
  void testBadFormat() {
    assertThrows(IOException.class, () -> new IntReply("*"));
  }

  @Test
  void testValid() throws IOException {
    assertEquals(23, new IntReply(":23").getValue());
  }
}
