/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Map;
import javax.validation.constraints.NotNull;

public record AuthCheckResponse(@NotNull Map<String, Result> matches) {

  public enum Result {
    MATCH("match"),
    NO_MATCH("no-match"),
    INVALID("invalid");

    private final String clientCode;

    Result(final String clientCode) {
      this.clientCode = clientCode;
    }

    @JsonValue
    public String clientCode() {
      return clientCode;
    }
  }
}
