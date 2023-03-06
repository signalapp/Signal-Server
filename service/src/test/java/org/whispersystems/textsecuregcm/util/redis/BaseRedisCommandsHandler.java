/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.whispersystems.textsecuregcm.util.redis.RedisLuaScriptSandbox.tail;

import java.util.List;

/**
 * This class is to be extended with implementations of Redis commands as needed.
 */
public class BaseRedisCommandsHandler implements RedisCommandsHandler {

  @Override
  public Object redisCommand(final String command, final List<Object> args) {
    return switch (command) {
      case "SET" -> {
        assertTrue(args.size() > 2);
        yield set(args.get(0).toString(), args.get(1).toString(), tail(args, 2));
      }
      case "GET" -> {
        assertEquals(1, args.size());
        yield get(args.get(0).toString());
      }
      case "DEL" -> {
        assertTrue(args.size() > 1);
        yield del(args.get(0).toString());
      }
      default -> other(command, args);
    };
  }

  public Object set(final String key, final String value, final List<Object> tail) {
    return "OK";
  }

  public String get(final String key) {
    return null;

  }

  public int del(final String key) {
    return 0;
  }

  public Object other(final String command, final List<Object> args) {
    return "OK";
  }
}
