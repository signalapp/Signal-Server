/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.UUID;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class NonNormalizedAccountCrawlerListenerTest {

  @ParameterizedTest
  @MethodSource
  void hasNumberNormalized(final String number, final boolean expectNormalized) {
    final Account account = mock(Account.class);
    when(account.getUuid()).thenReturn(UUID.randomUUID());
    when(account.getNumber()).thenReturn(number);

    assertEquals(expectNormalized, NonNormalizedAccountCrawlerListener.hasNumberNormalized(account));
  }

  private static Stream<Arguments> hasNumberNormalized() {
    return Stream.of(
        Arguments.of("+447700900111", true),
        Arguments.of("+4407700900111", false),
        Arguments.of("Not a real phone number", false),
        Arguments.of(null, false)
    );
  }
}
