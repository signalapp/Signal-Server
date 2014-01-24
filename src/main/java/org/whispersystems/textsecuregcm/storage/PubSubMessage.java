package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class PubSubMessage {

  public static final int TYPE_QUERY_DB = 1;
  public static final int TYPE_DELIVER  = 2;

  @JsonProperty
  private int type;

  @JsonProperty
  private String contents;

  public PubSubMessage() {}

  public PubSubMessage(int type, String contents) {
    this.type     = type;
    this.contents = contents;
  }

  public int getType() {
    return type;
  }

  public String getContents() {
    return contents;
  }
}
