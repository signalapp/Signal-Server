/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

public class OutgoingMessageEntity {

  private final UUID guid;
  private final int type;
  private final long timestamp;
  private final String source;
  private final UUID sourceUuid;
  private final int sourceDevice;
  private final UUID destinationUuid;
  private final byte[] content;
  private final long serverTimestamp;

  @JsonCreator
  public OutgoingMessageEntity(@JsonProperty("guid") final UUID guid,
      @JsonProperty("type") final int type,
      @JsonProperty("timestamp") final long timestamp,
      @JsonProperty("source") final String source,
      @JsonProperty("sourceUuid") final UUID sourceUuid,
      @JsonProperty("sourceDevice") final int sourceDevice,
      @JsonProperty("destinationUuid") final UUID destinationUuid,
      @JsonProperty("content") final byte[] content,
      @JsonProperty("serverTimestamp") final long serverTimestamp)
  {
    this.guid            = guid;
    this.type            = type;
    this.timestamp       = timestamp;
    this.source          = source;
    this.sourceUuid      = sourceUuid;
    this.sourceDevice    = sourceDevice;
    this.destinationUuid = destinationUuid;
    this.content         = content;
    this.serverTimestamp = serverTimestamp;
  }

  public UUID getGuid() {
    return guid;
  }

  public int getType() {
    return type;
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

  public byte[] getContent() {
    return content;
  }

  public long getServerTimestamp() {
    return serverTimestamp;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    final OutgoingMessageEntity that = (OutgoingMessageEntity)o;
    return type == that.type &&
            timestamp == that.timestamp &&
            sourceDevice == that.sourceDevice &&
            serverTimestamp == that.serverTimestamp &&
            guid.equals(that.guid) &&
            Objects.equals(source, that.source) &&
            Objects.equals(sourceUuid, that.sourceUuid) &&
            destinationUuid.equals(that.destinationUuid) &&
            Arrays.equals(content, that.content);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(guid, type, timestamp, source, sourceUuid, sourceDevice, destinationUuid, serverTimestamp);
    result = 31 * result + Arrays.hashCode(content);
    return result;
  }
}
