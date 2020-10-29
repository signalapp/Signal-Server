/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.hibernate.validator.constraints.NotEmpty;

public class GcmRegistrationId {

  @JsonProperty
  @NotEmpty
  private String gcmRegistrationId;

  public GcmRegistrationId() {}

  @VisibleForTesting
  public GcmRegistrationId(String id) {
    this.gcmRegistrationId = id;
  }

  public String getGcmRegistrationId() {
    return gcmRegistrationId;
  }


}

