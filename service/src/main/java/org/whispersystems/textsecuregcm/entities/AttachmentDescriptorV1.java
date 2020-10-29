/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AttachmentDescriptorV1 {

  @JsonProperty
  private long id;

  @JsonProperty
  private String idString;

  @JsonProperty
  private String location;

  public AttachmentDescriptorV1(long id, String location) {
    this.id       = id;
    this.idString = String.valueOf(id);
    this.location = location;
  }

  public AttachmentDescriptorV1() {}

  public long getId() {
    return id;
  }

  public String getLocation() {
    return location;
  }

  public String getIdString() {
    return idString;
  }
}
