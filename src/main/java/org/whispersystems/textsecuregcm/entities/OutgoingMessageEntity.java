package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OutgoingMessageEntity {

  @JsonIgnore
  private long id;

  @JsonIgnore
  private boolean cached;

  @JsonProperty
  private int type;

  @JsonProperty
  private String relay;

  @JsonProperty
  private long timestamp;

  @JsonProperty
  private String source;

  @JsonProperty
  private int sourceDevice;

  @JsonProperty
  private byte[] message;

  @JsonProperty
  private byte[] content;

  public OutgoingMessageEntity() {}

  public OutgoingMessageEntity(long id, boolean cached, int type, String relay, long timestamp,
                               String source, int sourceDevice, byte[] message,
                               byte[] content)
  {
    this.id           = id;
    this.cached       = cached;
    this.type         = type;
    this.relay        = relay;
    this.timestamp    = timestamp;
    this.source       = source;
    this.sourceDevice = sourceDevice;
    this.message      = message;
    this.content      = content;
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

}
