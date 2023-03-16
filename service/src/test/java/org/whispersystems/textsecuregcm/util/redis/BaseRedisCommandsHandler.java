/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.redis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.whispersystems.textsecuregcm.util.redis.RedisLuaScriptSandbox.tail;

import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * This class is to be extended with implementations of Redis commands as needed.
 */
public class BaseRedisCommandsHandler implements RedisCommandsHandler {

  @Override
  public Object redisCommand(final String command, final List<Object> args) {
    return switch (command.toUpperCase(Locale.ROOT)) {
      case "SET" -> {
        assertTrue(args.size() > 2);
        yield set(args.get(0).toString(), args.get(1).toString(), tail(args, 2));
      }
      case "GET" -> {
        assertEquals(1, args.size());
        yield get(args.get(0).toString());
      }
      case "DEL" -> {
        assertTrue(args.size() >= 1);
        yield del(args.stream().map(Object::toString).toList());
      }
      case "HSET" -> {
        assertTrue(args.size() > 1);
        assertTrue(args.size() % 2 == 1);
        yield hset(args.get(0).toString(), tail(args, 1));
      }
      case "HGET" -> {
        assertEquals(2, args.size());
        yield hget(args.get(0).toString(), args.get(1).toString());
      }
      case "HMGET" -> {
        assertTrue(args.size() > 1);
        yield hmget(args.get(0).toString(), tail(args, 1));
      }
      case "PEXPIRE" -> {
        assertEquals(2, args.size());
        yield pexpire(args.get(0).toString(), Double.valueOf(args.get(1).toString()).longValue(), tail(args, 2));
      }
      case "TYPE" -> {
        assertEquals(1, args.size());
        yield type(args.get(0).toString());
      }
      case "RPUSH" -> {
        assertTrue(args.size() > 1);
        yield push(false, args.get(0).toString(), tail(args, 1));
      }
      case "LPUSH" -> {
        assertTrue(args.size() > 1);
        yield push(true, args.get(0).toString(), tail(args, 1));
      }
      case "RPOP" -> {
        assertEquals(2, args.size());
        yield pop(false, args.get(0).toString(), Double.valueOf(args.get(1).toString()).intValue());
      }
      case "LPOP" -> {
        assertEquals(2, args.size());
        yield pop(true, args.get(0).toString(), Double.valueOf(args.get(1).toString()).intValue());
      }

      default -> other(command, args);
    };
  }

  public Object[] pop(final boolean left, final String key, final int count) {
    return new Object[count];
  }

  public Object push(final boolean left, final String key, final List<Object> values) {
    return 0;
  }

  public Object type(final String key) {
    return Map.of("ok", "none");
  }

  public Object pexpire(final String key, final long ttlMillis, final List<Object> args) {
    return 0;
  }

  public Object hset(final String key, final List<Object> fieldsAndValues) {
    return "OK";
  }

  public Object hget(final String key, final String field) {
    return null;
  }

  public Object[] hmget(final String key, final List<Object> fields) {
    return new Object[fields.size()];
  }

  public Object set(final String key, final String value, final List<Object> tail) {
    return "OK";
  }

  public String get(final String key) {
    return null;
  }

  public int del(final List<String> keys) {
    return 0;
  }

  public Object other(final String command, final List<Object> args) {
    return "OK";
  }
}
