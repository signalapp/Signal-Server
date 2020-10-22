/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.dispatch;

public interface DispatchChannel {
  public void onDispatchMessage(String channel, byte[] message);
  public void onDispatchSubscribed(String channel);
  public void onDispatchUnsubscribed(String channel);
}
