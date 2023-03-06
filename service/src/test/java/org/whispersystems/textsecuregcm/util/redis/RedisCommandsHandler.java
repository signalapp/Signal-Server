/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.redis;

import java.util.List;

@FunctionalInterface
public interface RedisCommandsHandler {

  Object redisCommand(String command, List<Object> args);
}
