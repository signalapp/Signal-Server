package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;

public class AttachmentDescriptorV3 {

  @JsonProperty
  private int cdn;

  @JsonProperty
  private String key;

  @JsonProperty
  private Map<String, String> headers;

  @JsonProperty
  private String signedUploadLocation;

  public AttachmentDescriptorV3() {
  }

  public AttachmentDescriptorV3(int cdn, String key, Map<String, String> headers, String signedUploadLocation) {
    this.cdn                  = cdn;
    this.key                  = key;
    this.headers              = headers;
    this.signedUploadLocation = signedUploadLocation;
  }

  public int getCdn() {
    return cdn;
  }

  public String getKey() {
    return key;
  }

  public Map<String, String> getHeaders() {
    return headers;
  }

  public String getSignedUploadLocation() {
    return signedUploadLocation;
  }
}
