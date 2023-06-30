/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.lettuce.core.RedisURI;
import java.time.Duration;

public class RedisUriUtil {

  public static RedisURI createRedisUriWithTimeout(final String uri, final Duration timeout) {
    final RedisURI redisUri = RedisURI.create(uri);
    // for synchronous commands and the initial connection
    redisUri.setTimeout(timeout);
    return redisUri;
  }

}
