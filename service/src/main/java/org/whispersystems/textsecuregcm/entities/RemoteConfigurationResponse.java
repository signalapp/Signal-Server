/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.v3.oas.annotations.media.Schema;
import java.util.Map;

public record RemoteConfigurationResponse(
  @JsonProperty
  @Schema(description = "Remote configurations applicable to the user and client")
  Map<String, String> config) {
}
