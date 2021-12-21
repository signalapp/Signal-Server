/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.dispatch.redis.protocol;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.IOException;
import org.junit.jupiter.api.Test;

class ArrayReplyHeaderTest {

  @Test
  void testNull() {
    assertThrows(IOException.class, () -> new ArrayReplyHeader(null));
  }

  @Test
  void testBadPrefix() {
    assertThrows(IOException.class, () -> new ArrayReplyHeader(":3"));
  }

  @Test
  void testEmpty() {
    assertThrows(IOException.class, () -> new ArrayReplyHeader(""));
  }

  @Test
  void testTruncated() {
    assertThrows(IOException.class, () -> new ArrayReplyHeader("*"));
  }

  @Test
  void testBadNumber() {
    assertThrows(IOException.class, () -> new ArrayReplyHeader("*ABC"));
  }

  @Test
  void testValid() throws IOException {
    assertEquals(4, new ArrayReplyHeader("*4").getElementCount());
  }









}
