/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import java.util.List;
import java.util.Locale.LanguageRange;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class LocaleTest {

  private static final Set<String> SUPPORTED_LOCALES = Set.of("es", "en", "zh", "zh-HK");

  @ParameterizedTest
  @MethodSource
  void testFindBestLocale(@Nullable final String languageRange, @Nullable final String expectedLocale) {

    final List<LanguageRange> languageRanges = Optional.ofNullable(languageRange)
        .map(LanguageRange::parse)
        .orElse(Collections.emptyList());

    assertEquals(Optional.ofNullable(expectedLocale), Util.findBestLocale(languageRanges, SUPPORTED_LOCALES));
  }

  static Stream<Arguments> testFindBestLocale() {
    return Stream.of(
        // languageRange, expectedLocale
        Arguments.of("en-US, fr", "en"),
        Arguments.of("es-ES", "es"),
        Arguments.of("zh-Hant-HK, zh-HK", "zh"),
        // zh-HK is supported, but Locale#lookup truncates from the end, per RFC-4647
        Arguments.of("zh-Hant-HK", "zh"),
        Arguments.of("zh-HK", "zh-HK"),
        Arguments.of("de", null),
        Arguments.of(null, null)
    );
  }
}
