/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.dispatch.io;

import org.whispersystems.dispatch.redis.PubSubConnection;

public interface RedisPubSubConnectionFactory {

  public PubSubConnection connect();

}
