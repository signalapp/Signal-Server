package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProfileAvatarUploadAttributes {

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

  public ProfileAvatarUploadAttributes() {}

  public ProfileAvatarUploadAttributes(String key, String credential,
                                       String acl,  String algorithm,
                                       String date, String policy,
                                       String signature)
  {
    this.key        = key;
    this.credential = credential;
    this.acl        = acl;
    this.algorithm  = algorithm;
    this.date       = date;
    this.policy     = policy;
    this.signature  = signature;
  }

}
