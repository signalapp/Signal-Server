/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;

@Schema(description = "Profile avatar upload form (S3 Post Policy, AWS Signature version 4)")
public class ProfileAvatarUploadAttributes {

  @Schema(description = "Object key for the avatar")
  @JsonProperty
  private String key;

  @Schema(description = "Credential for the upload")
  @JsonProperty
  private String credential;

  @Schema(description = "Access control list setting")
  @JsonProperty
  private String acl;

  @Schema(description = "Signing algorithm")
  @JsonProperty
  private String algorithm;

  @Schema(description = "Date in AWS format")
  @JsonProperty
  private String date;

  @Schema(description = "Base64-encoded upload policy")
  @JsonProperty
  private String policy;

  @Schema(description = "Signature calculated over the policy")
  @JsonProperty
  private String signature;

  public ProfileAvatarUploadAttributes() {}

  public ProfileAvatarUploadAttributes(String key, String credential, String acl, String algorithm, String date,
      String policy, String signature) {

    this.key = key;
    this.credential = credential;
    this.acl = acl;
    this.algorithm = algorithm;
    this.date = date;
    this.policy = policy;
    this.signature = signature;
  }

  public String getKey() {
    return key;
  }

}
