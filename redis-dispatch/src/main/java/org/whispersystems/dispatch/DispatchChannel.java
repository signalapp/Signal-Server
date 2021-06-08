/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.dispatch;

public interface DispatchChannel {
  void onDispatchMessage(String channel, byte[] message);
  void onDispatchSubscribed(String channel);
  void onDispatchUnsubscribed(String channel);
}
