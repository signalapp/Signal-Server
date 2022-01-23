/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.gcm.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.whispersystems.gcm.server.util.JsonHelpers.jsonFixture;

import java.io.IOException;
import org.junit.jupiter.api.Test;

public class MessageTest {

  @Test
  void testMinimal() throws IOException {
    Message message = Message.newBuilder()
                             .withDestination("1")
                             .build();

    assertEquals(message.serialize(), jsonFixture("fixtures/message-minimal.json"));
  }

  @Test
  void testComplete() throws IOException {
    Message message = Message.newBuilder()
                             .withDestination("1")
                             .withCollapseKey("collapse")
                             .withDelayWhileIdle(true)
                             .withTtl(10)
                             .withPriority("high")
                             .build();

    assertEquals(message.serialize(), jsonFixture("fixtures/message-complete.json"));
  }

  @Test
  void testWithData() throws IOException {
    Message message = Message.newBuilder()
                             .withDestination("2")
                             .withDataPart("key1", "value1")
                             .withDataPart("key2", "value2")
                             .build();

    assertEquals(message.serialize(), jsonFixture("fixtures/message-data.json"));
  }

}
