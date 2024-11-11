/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import jakarta.validation.constraints.DecimalMax;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.whispersystems.textsecuregcm.captcha.Action;

public class DynamicCaptchaConfiguration {

  @JsonProperty
  @DecimalMin("0")
  @DecimalMax("1")
  @NotNull
  private BigDecimal scoreFloor;

  @JsonProperty
  private boolean allowHCaptcha = false;

  @JsonProperty
  @NotNull
  private Map<Action, Set<String>> hCaptchaSiteKeys = Collections.emptyMap();

  @JsonProperty
  @NotNull
  private Map<Action, BigDecimal> scoreFloorByAction = Collections.emptyMap();

  public BigDecimal getScoreFloor() {
    return scoreFloor;
  }

  public boolean isAllowHCaptcha() {
    return allowHCaptcha;
  }

  public Map<Action, BigDecimal> getScoreFloorByAction() {
    return scoreFloorByAction;
  }

  @VisibleForTesting
  public void setAllowHCaptcha(final boolean allowHCaptcha) {
    this.allowHCaptcha = allowHCaptcha;
  }

  @VisibleForTesting
  public void setScoreFloor(final BigDecimal scoreFloor) {
    this.scoreFloor = scoreFloor;
  }

  public Map<Action, Set<String>> getHCaptchaSiteKeys() {
    return hCaptchaSiteKeys;
  }

  @VisibleForTesting
  public void setHCaptchaSiteKeys(final Map<Action, Set<String>> hCaptchaSiteKeys) {
    this.hCaptchaSiteKeys = hCaptchaSiteKeys;
  }

}
