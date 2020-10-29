/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

public class DirectoryReconciliationResponse {

  @JsonProperty
  @NotEmpty
  private Status status;

  public DirectoryReconciliationResponse() {
  }

  public DirectoryReconciliationResponse(Status status) {
    this.status = status;
  }

  public Status getStatus() {
    return status;
  }

  public enum Status {
    OK,
    MISSING,
  }

}
