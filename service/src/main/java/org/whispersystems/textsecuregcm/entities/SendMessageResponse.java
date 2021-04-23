/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.UUID;

public class SendMessageResponse {

  @JsonProperty
  private boolean needsSync;

  @JsonProperty
  private List<UUID> uuids404;

  public SendMessageResponse() {}

  public SendMessageResponse(boolean needsSync) {
    this.needsSync = needsSync;
  }

  public SendMessageResponse(List<UUID> uuids404) {
    this.uuids404 = uuids404;
  }
}
