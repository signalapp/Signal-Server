/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public class OutgoingMessageEntity {

  @JsonIgnore
  private final long id;

  @JsonIgnore
  private final boolean cached;

  @JsonProperty
  private final UUID guid;

  @JsonProperty
  private final int type;

  @JsonProperty
  private final String relay;

  @JsonProperty
  private final long timestamp;

  @JsonProperty
  private final String source;

  @JsonProperty
  private final UUID sourceUuid;

  @JsonProperty
  private final int sourceDevice;

  @JsonProperty
  private final UUID destinationUuid;

  @JsonProperty
  private final byte[] message;

  @JsonProperty
  private final byte[] content;

  @JsonProperty
  private final long serverTimestamp;

  @JsonCreator
  public OutgoingMessageEntity(@JsonProperty("id") final long id,
      @JsonProperty("cached") final boolean cached,
      @JsonProperty("guid") final UUID guid,
      @JsonProperty("type") final int type,
      @JsonProperty("relay") final String relay,
      @JsonProperty("timestamp") final long timestamp,
      @JsonProperty("source") final String source,
      @JsonProperty("sourceUuid") final UUID sourceUuid,
      @JsonProperty("sourceDevice") final int sourceDevice,
      @JsonProperty("destinationUuid") final UUID destinationUuid,
      @JsonProperty("message") final byte[] message,
      @JsonProperty("content") final byte[] content,
      @JsonProperty("serverTimestamp") final long serverTimestamp)
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
    this.destinationUuid = destinationUuid;
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

  public UUID getDestinationUuid() {
    return destinationUuid;
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
