/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

public record AttachmentDescriptorV2(long attachmentId,
                                     String key,
                                     String credential,
                                     String acl,
                                     String algorithm,
                                     String date,
                                     String policy,
                                     String signature) {

  @JsonProperty
  public String attachmentIdString() {
    return String.valueOf(attachmentId);
  }
}
