/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.function.Function;
import java.util.stream.Collectors;

public class EnumMapUtil {

  private EnumMapUtil() {}

  public static <E extends Enum<E>, V> EnumMap<E, V> toEnumMap(final Class<E> enumClass, final Function<E, V> valueMapper) {
    return Arrays.stream(enumClass.getEnumConstants())
        .collect(Collectors.toMap(Function.identity(), valueMapper, (a, b) -> {
              throw new AssertionError("Duplicate enumeration key");
            },
            () -> new EnumMap<>(enumClass)));
  }
}
