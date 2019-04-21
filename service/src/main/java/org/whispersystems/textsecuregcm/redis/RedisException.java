package org.whispersystems.textsecuregcm.redis;

public class RedisException extends Exception {

  public RedisException(Exception e) {
    super(e);
  }
}
