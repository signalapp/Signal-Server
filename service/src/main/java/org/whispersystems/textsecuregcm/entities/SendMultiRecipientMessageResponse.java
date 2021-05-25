/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.UUID;

public class SendMultiRecipientMessageResponse {
  @JsonProperty
  private List<UUID> uuids404;

  public SendMultiRecipientMessageResponse() {
  }

  public SendMultiRecipientMessageResponse(final List<UUID> uuids404) {
    this.uuids404 = uuids404;
  }
}
