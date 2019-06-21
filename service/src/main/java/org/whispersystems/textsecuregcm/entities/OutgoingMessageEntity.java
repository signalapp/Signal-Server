package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

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

}
