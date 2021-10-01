/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import javax.validation.constraints.NotNull;

public class RedeemedReceiptsDynamoDbConfiguration extends DynamoDbConfiguration {

  private Duration expirationTime;

  @NotNull
  @JsonProperty
  public Duration getExpirationTime() {
    return expirationTime;
  }
}
