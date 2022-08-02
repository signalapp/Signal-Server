/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import static org.junit.jupiter.api.Assertions.*;

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
          SystemMapper.getMapper().readValue(incomingMessageListJson, IncomingMessageList.class);

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
          SystemMapper.getMapper().readValue(incomingMessageListJson, IncomingMessageList.class);

      assertTrue(incomingMessageList.online());
      assertTrue(incomingMessageList.urgent());
    }
  }
}
