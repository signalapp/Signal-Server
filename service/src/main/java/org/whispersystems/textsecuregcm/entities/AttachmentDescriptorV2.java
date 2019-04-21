package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AttachmentDescriptorV2 {

  @JsonProperty
  private String key;

  @JsonProperty
  private String credential;

  @JsonProperty
  private String acl;

  @JsonProperty
  private String algorithm;

  @JsonProperty
  private String date;

  @JsonProperty
  private String policy;

  @JsonProperty
  private String signature;

  @JsonProperty
  private long attachmentId;

  @JsonProperty
  private String attachmentIdString;

  public AttachmentDescriptorV2() {}

  public AttachmentDescriptorV2(long attachmentId,
                                String key, String credential,
                                String acl,  String algorithm,
                                String date, String policy,
                                String signature)
  {
    this.attachmentId       = attachmentId;
    this.attachmentIdString = String.valueOf(attachmentId);
    this.key                = key;
    this.credential         = credential;
    this.acl                = acl;
    this.algorithm          = algorithm;
    this.date               = date;
    this.policy             = policy;
    this.signature          = signature;
  }

  public String getKey() {
    return key;
  }

  public String getCredential() {
    return credential;
  }

  public String getAcl() {
    return acl;
  }

  public String getAlgorithm() {
    return algorithm;
  }

  public String getDate() {
    return date;
  }

  public String getPolicy() {
    return policy;
  }

  public String getSignature() {
    return signature;
  }

  public long getAttachmentId() {
    return attachmentId;
  }

  public String getAttachmentIdString() {
    return attachmentIdString;
  }
}
