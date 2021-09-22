/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.lettuce.core.RedisException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisOperation {

  private static final Logger logger = LoggerFactory.getLogger(RedisOperation.class);

  /**
   * Executes the given task and logs and discards any {@link RedisException} that may be thrown. This method should be
   * used for best-effort tasks like gathering metrics.
   *
   * @param runnable the Redis-related task to be executed
   */
  public static void unchecked(final Runnable runnable) {
    try {
      runnable.run();
    } catch (RedisException e) {
      logger.warn("Redis failure", e);
    }
  }
}
