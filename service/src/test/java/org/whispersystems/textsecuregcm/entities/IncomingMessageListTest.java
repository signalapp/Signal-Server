/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.SystemMapper;

class IncomingMessageListTest {

  @Test
  void fromJson() throws JsonProcessingException {
    {
      final String incomingMessageListJson = """
          {
            "messages": [],
            "timestamp": 123456789,
            "online": true,
            "urgent": false
          }
          """;

      final IncomingMessageList incomingMessageList =
          SystemMapper.jsonMapper().readValue(incomingMessageListJson, IncomingMessageList.class);

      assertTrue(incomingMessageList.online());
      assertFalse(incomingMessageList.urgent());
    }

    {
      final String incomingMessageListJson = """
          {
            "messages": [],
            "timestamp": 123456789,
            "online": true
          }
          """;

      final IncomingMessageList incomingMessageList =
          SystemMapper.jsonMapper().readValue(incomingMessageListJson, IncomingMessageList.class);

      assertTrue(incomingMessageList.online());
      assertTrue(incomingMessageList.urgent());
    }
  }
}
