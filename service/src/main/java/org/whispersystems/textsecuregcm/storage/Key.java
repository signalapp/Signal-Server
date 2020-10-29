/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.io.IOException;

class Key {

  private final byte[] userMessageQueue;
  private final byte[] userMessageQueueMetadata;
  private final byte[] userMessageQueuePersistInProgress;

  private final String address;
  private final long   deviceId;

  Key(String address, long deviceId) {
    this.address                           = address;
    this.deviceId                          = deviceId;
    this.userMessageQueue                  = ("user_queue::" + address + "::" + deviceId).getBytes();
    this.userMessageQueueMetadata          = ("user_queue_metadata::" + address + "::" + deviceId).getBytes();
    this.userMessageQueuePersistInProgress = ("user_queue_persisting::" + address + "::" + deviceId).getBytes();
  }

  String getAddress() {
    return address;
  }

  long getDeviceId() {
    return deviceId;
  }

  byte[] getUserMessageQueue() {
    return userMessageQueue;
  }

  byte[] getUserMessageQueueMetadata() {
    return userMessageQueueMetadata;
  }

  byte[] getUserMessageQueuePersistInProgress() {
    return userMessageQueuePersistInProgress;
  }

  static byte[] getUserMessageQueueIndex() {
    return "user_queue_index".getBytes();
  }

  static Key fromUserMessageQueue(byte[] userMessageQueue) throws IOException {
    try {
      String[] parts = new String(userMessageQueue).split("::");

      if (parts.length != 3) {
        throw new IOException("Malformed key: " + new String(userMessageQueue));
      }

      return new Key(parts[1], Long.parseLong(parts[2]));
    } catch (NumberFormatException e) {
      throw new IOException(e);
    }
  }
}
