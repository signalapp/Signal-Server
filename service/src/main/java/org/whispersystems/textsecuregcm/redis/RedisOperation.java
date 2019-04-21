package org.whispersystems.textsecuregcm.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.push.PushSender;

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
