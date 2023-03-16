/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.redis;

import java.time.Clock;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nullable;

public class SimpleCacheCommandsHandler extends BaseRedisCommandsHandler {

  public record Entry(Object value, long expirationEpochMillis) {
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
    return getIfNotExpired(key, String.class);
  }

  @Override
  public int del(final List<String> key) {
    return key.stream()
        .mapToInt(k -> cache.remove(k) != null ? 1 : 0)
        .sum();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object hset(final String key, final List<Object> fieldsAndValues) {
    Map<Object, Object> map = getIfNotExpired(key, Map.class);
    if (map == null) {
      map = new ConcurrentHashMap<>();
      cache.put(key, new Entry(map, Long.MAX_VALUE));
    }
    final Iterator<Object> iter = fieldsAndValues.iterator();
    while (iter.hasNext()) {
      final Object k = iter.next();
      final Object v = iter.next();
      map.put(k, v);
    }
    return "OK";
  }

  @Override
  public Object hget(final String key, final String field) {
    final Map<?, ?> map = getIfNotExpired(key, Map.class);
    return map == null ? null : map.get(field);
  }

  @Override
  public Object[] hmget(final String key, final List<Object> fields) {
    final Object[] res = new Object[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      res[i] = hget(key, fields.get(i).toString());
    }
    return res;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object push(final boolean left, final String key, final List<Object> values) {
    LinkedList<Object> list = getIfNotExpired(key, LinkedList.class);
    if (list == null) {
      list = new LinkedList<>();
      cache.put(key, new Entry(list, Long.MAX_VALUE));
    }
    for (Object v: values) {
      if (left) {
        list.addFirst(v.toString());
      } else {
        list.addLast(v.toString());
      }
    }
    return list.size();
  }

  @SuppressWarnings("unchecked")
  @Override
  public Object[] pop(final boolean left, final String key, final int count) {
    final Object[] result = new String[count];
    final LinkedList<Object> list = getIfNotExpired(key, LinkedList.class);
    if (list == null) {
      return result;
    }
    for (int i = 0; i < Math.min(count, list.size()); i++) {
      result[i] = left ? list.removeFirst() : list.removeLast();
    }
    return result;
  }

  @Override
  public Object pexpire(final String key, final long ttlMillis, final List<Object> args) {
    final Entry e = cache.get(key);
    if (e == null) {
      return 0;
    }
    final Entry updated = new Entry(e.value(), clock.millis() + ttlMillis);
    cache.put(key, updated);
    return 1;
  }

  @Override
  public Object type(final String key) {
    final Object o = getIfNotExpired(key, Object.class);
    final String type;
    if (o == null) {
      type = "none";
    } else if (o.getClass() == String.class) {
      type = "string";
    } else if (Map.class.isAssignableFrom(o.getClass())) {
      type = "hash";
    } else if (List.class.isAssignableFrom(o.getClass())) {
      type = "list";
    } else {
      throw new IllegalArgumentException("Unsupported value type: " + o.getClass());
    }
    return Map.of("ok", type);
  }

  @Nullable
  protected <T> T getIfNotExpired(final String key, final Class<T> expectedType) {
    final Entry entry = cache.get(key);
    if (entry == null) {
      return null;
    }
    if (entry.expirationEpochMillis() < clock.millis()) {
      del(List.of(key));
      return null;
    }
    return expectedType.cast(entry.value());
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
