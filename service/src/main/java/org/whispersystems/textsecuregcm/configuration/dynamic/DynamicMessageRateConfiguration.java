/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;

public class DynamicMessageRateConfiguration {

  @JsonProperty
  private boolean enforceUnsealedSenderRateLimit = false;

  @JsonProperty
  private Set<String> rateLimitedCountryCodes = Collections.emptySet();

  @JsonProperty
  private Set<String> rateLimitedHosts = Collections.emptySet();

  @JsonProperty
  private Duration responseDelay = Duration.ofNanos(1_200_000);

  @JsonProperty
  private Duration responseDelayJitter = Duration.ofNanos(500_000);

  @JsonProperty
  private Duration receiptDelay = Duration.ofMillis(1_200);

  @JsonProperty
  private Duration receiptDelayJitter = Duration.ofMillis(800);

  @JsonProperty
  private double receiptProbability = 0.82;

  public boolean isEnforceUnsealedSenderRateLimit() {
    return enforceUnsealedSenderRateLimit;
  }

  public Set<String> getRateLimitedCountryCodes() {
    return rateLimitedCountryCodes;
  }

  public Set<String> getRateLimitedHosts() {
    return rateLimitedHosts;
  }

  public Duration getResponseDelay() {
    return responseDelay;
  }

  public Duration getResponseDelayJitter() {
    return responseDelayJitter;
  }

  public Duration getReceiptDelay() {
    return receiptDelay;
  }

  public Duration getReceiptDelayJitter() {
    return receiptDelayJitter;
  }

  public double getReceiptProbability() {
    return receiptProbability;
  }
}
