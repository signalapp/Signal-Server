/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.logging;

import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UncaughtExceptionHandler {

  private static final Logger logger = LoggerFactory.getLogger(UncaughtExceptionHandler.class);

  public static void register() {
    @Nullable final Thread.UncaughtExceptionHandler current = Thread.getDefaultUncaughtExceptionHandler();

    if (current != null) {
      logger.warn("Uncaught exception handler already exists: {}", current);
      return;
    }

    Thread.setDefaultUncaughtExceptionHandler((t, e) -> logger.error("Uncaught exception on thread {}", t, e));
  }

}
