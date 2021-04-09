/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public class OutgoingMessageEntity {

  @JsonIgnore
  private long id;

  @JsonIgnore
  private boolean cached;

  @JsonProperty
  private UUID guid;

  @JsonProperty
  private int type;

  @JsonProperty
  private String relay;

  @JsonProperty
  private long timestamp;

  @JsonProperty
  private String source;

  @JsonProperty
  private UUID sourceUuid;

  @JsonProperty
  private int sourceDevice;

  @JsonProperty
  private byte[] message;

  @JsonProperty
  private byte[] content;

  @JsonProperty
  private long serverTimestamp;

  public OutgoingMessageEntity() {}

  public OutgoingMessageEntity(long id, boolean cached,
                               UUID guid, int type, String relay, long timestamp,
                               String source, UUID sourceUuid, int sourceDevice,
                               byte[] message, byte[] content, long serverTimestamp)
  {
    this.id              = id;
    this.cached          = cached;
    this.guid            = guid;
    this.type            = type;
    this.relay           = relay;
    this.timestamp       = timestamp;
    this.source          = source;
    this.sourceUuid      = sourceUuid;
    this.sourceDevice    = sourceDevice;
    this.message         = message;
    this.content         = content;
    this.serverTimestamp = serverTimestamp;
  }

  public UUID getGuid() {
    return guid;
  }

  public int getType() {
    return type;
  }

  public String getRelay() {
    return relay;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getSource() {
    return source;
  }

  public UUID getSourceUuid() {
    return sourceUuid;
  }

  public int getSourceDevice() {
    return sourceDevice;
  }

  public byte[] getMessage() {
    return message;
  }

  public byte[] getContent() {
    return content;
  }

  @JsonIgnore
  public long getId() {
    return id;
  }

  @JsonIgnore
  public boolean isCached() {
    return cached;
  }

  public long getServerTimestamp() {
    return serverTimestamp;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final OutgoingMessageEntity that = (OutgoingMessageEntity)o;
    return id == that.id &&
            cached == that.cached &&
            type == that.type &&
            timestamp == that.timestamp &&
            sourceDevice == that.sourceDevice &&
            serverTimestamp == that.serverTimestamp &&
            Objects.equals(guid, that.guid) &&
            Objects.equals(relay, that.relay) &&
            Objects.equals(source, that.source) &&
            Objects.equals(sourceUuid, that.sourceUuid) &&
            Arrays.equals(message, that.message) &&
            Arrays.equals(content, that.content);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(id, cached, guid, type, relay, timestamp, source, sourceUuid, sourceDevice, serverTimestamp);
    result = 31 * result + Arrays.hashCode(message);
    result = 31 * result + Arrays.hashCode(content);
    return result;
  }
}
