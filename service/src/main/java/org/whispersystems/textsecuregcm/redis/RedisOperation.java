/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RedisOperation {

  private static final Logger logger = LoggerFactory.getLogger(RedisOperation.class);

  public static void unchecked(Operation operation) {
    try {
      operation.run();
    } catch (RedisException e) {
      logger.warn("Jedis failure", e);
    }
  }

  public static boolean unchecked(BooleanOperation operation) {
    try {
      return operation.run();
    } catch (RedisException e) {
      logger.warn("Jedis failure", e);
    }

    return false;
  }

  @FunctionalInterface
  public interface Operation {
    public void run() throws RedisException;
  }

  public interface BooleanOperation {
    public boolean run() throws RedisException;
  }
}
