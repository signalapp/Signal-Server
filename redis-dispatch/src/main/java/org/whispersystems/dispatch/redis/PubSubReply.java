/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.dispatch.redis;

import java.util.Optional;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class PubSubReply {

  public enum Type {
    MESSAGE,
    SUBSCRIBE,
    UNSUBSCRIBE
  }

  private final Type             type;
  private final String           channel;
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
