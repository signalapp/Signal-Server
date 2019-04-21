package org.whispersystems.dispatch.io;

import org.whispersystems.dispatch.redis.PubSubConnection;

public interface RedisPubSubConnectionFactory {

  public PubSubConnection connect();

}
