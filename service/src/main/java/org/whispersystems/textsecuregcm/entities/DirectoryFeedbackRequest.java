/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Size;
import javax.validation.valueextraction.Unwrapping;
import java.util.Optional;

public class DirectoryFeedbackRequest {

  @Size(max = 1024, payload = {Unwrapping.Unwrap.class})
  @JsonProperty
  private Optional<String> reason;

  public DirectoryFeedbackRequest() {
  }

  public DirectoryFeedbackRequest(Optional<String> reason) {
    this.reason = reason;
  }

  public Optional<String> getReason() {
    return reason;
  }

}
