/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.redis;

import java.time.Clock;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SimpleCacheCommandsHandler extends BaseRedisCommandsHandler {

  public record Entry(String value, long expirationEpochMillis) {
  }

  private final Map<String, Entry> cache = new ConcurrentHashMap<>();

  private final Clock clock;


  public SimpleCacheCommandsHandler(final Clock clock) {
    this.clock = clock;
  }

  @Override
  public Object set(final String key, final String value, final List<Object> tail) {
    cache.put(key, new Entry(value, resolveExpirationEpochMillis(tail)));
    return "OK";
  }

  @Override
  public String get(final String key) {
    final Entry entry = cache.get(key);
    if (entry == null) {
      return null;
    }
    if (entry.expirationEpochMillis() < clock.millis()) {
      del(key);
      return null;
    }
    return entry.value();
  }

  @Override
  public int del(final String key) {
    return cache.remove(key) != null ? 1 : 0;
  }

  protected long resolveExpirationEpochMillis(final List<Object> args) {
    for (int i = 0; i < args.size() - 1; i++) {
      final long currentTimeMillis = clock.millis();
      final String param = args.get(i).toString();
      final String value = args.get(i + 1).toString();
      switch (param) {
        case "EX" -> {
          return currentTimeMillis + Double.valueOf(value).longValue() * 1000;
        }
        case "PX" -> {
          return currentTimeMillis + Double.valueOf(value).longValue();
        }
        case "EXAT" -> {
          return Double.valueOf(value).longValue() * 1000;
        }
        case "PXAT" -> {
          return Double.valueOf(value).longValue();
        }
      }
    }
    return Long.MAX_VALUE;
  }
}
