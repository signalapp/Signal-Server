/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;


import java.util.Optional;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class GcmMessage {

  public enum Type {
    NOTIFICATION, CHALLENGE
  }

  private final String           gcmId;
  private final String           number;
  private final int              deviceId;
  private final Type             type;
  private final Optional<String> data;

  public GcmMessage(String gcmId, String number, int deviceId, Type type, Optional<String> data) {
    this.gcmId    = gcmId;
    this.number   = number;
    this.deviceId = deviceId;
    this.type     = type;
    this.data     = data;
  }

  public String getGcmId() {
    return gcmId;
  }

  public String getNumber() {
    return number;
  }

  public Type getType() {
    return type;
  }

  public int getDeviceId() {
    return deviceId;
  }

  public Optional<String> getData() {
    return data;
  }

}
