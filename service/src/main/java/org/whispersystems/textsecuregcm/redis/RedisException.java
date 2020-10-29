/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

public class RedisException extends Exception {

  public RedisException(Exception e) {
    super(e);
  }
}
