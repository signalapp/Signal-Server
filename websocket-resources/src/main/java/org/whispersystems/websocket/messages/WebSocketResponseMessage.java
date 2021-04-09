/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.websocket.messages;


import java.util.Map;
import java.util.Optional;

public interface WebSocketResponseMessage {
  public long               getRequestId();
  public int                getStatus();
  public String             getMessage();
  public Map<String,String> getHeaders();
  public Optional<byte[]> getBody();
}
