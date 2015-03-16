package org.whispersystems.dispatch.redis;

import com.google.common.base.Optional;

public class PubSubReply {

  public enum Type {
    MESSAGE,
    SUBSCRIBE,
    UNSUBSCRIBE
  }

  private final Type             type;
  private final String          channel;
  private final Optional<byte[]> content;

  public PubSubReply(Type type, String channel, Optional<byte[]> content) {
    this.type    = type;
    this.channel = channel;
    this.content = content;
  }

  public Type getType() {
    return type;
  }

  public String getChannel() {
    return channel;
  }

  public Optional<byte[]> getContent() {
    return content;
  }

}
