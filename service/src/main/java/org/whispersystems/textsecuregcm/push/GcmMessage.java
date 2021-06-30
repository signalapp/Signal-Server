/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;


import javax.annotation.Nullable;
import java.util.Optional;
import java.util.UUID;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class GcmMessage {

  public enum Type {
    NOTIFICATION, CHALLENGE, RATE_LIMIT_CHALLENGE
  }

  private final String           gcmId;
  private final int              deviceId;
  private final Type             type;
  private final Optional<String> data;

  @Nullable
  private final UUID             uuid;

  public GcmMessage(String gcmId, @Nullable UUID uuid, int deviceId, Type type, Optional<String> data) {
    this.gcmId    = gcmId;
    this.uuid     = uuid;
    this.deviceId = deviceId;
    this.type     = type;
    this.data     = data;
  }

  public String getGcmId() {
    return gcmId;
  }

  public Optional<UUID> getUuid() {
    return Optional.ofNullable(uuid);
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
