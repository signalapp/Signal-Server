/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import org.mockito.Mockito;

public final class MockHelper {

  private MockHelper() {
    // utility class
  }

  @FunctionalInterface
  public interface MockInitializer<T> {

    void init(T mock) throws Exception;
  }

  public static <T> T buildMock(final Class<T> clazz, final MockInitializer<T> initializer) throws RuntimeException {
    final T mock = Mockito.mock(clazz);
    try {
      initializer.init(mock);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    return mock;
  }
}
