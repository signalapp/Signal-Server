/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import java.math.BigDecimal;
import javax.validation.constraints.DecimalMax;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

public class RecaptchaV2Configuration {

  private BigDecimal scoreFloor;
  private String projectPath;
  private String siteKey;
  private String credentialConfigurationJson;

  @DecimalMin("0")
  @DecimalMax("1")
  @NotNull
  public BigDecimal getScoreFloor() {
    return scoreFloor;
  }

  @NotEmpty
  public String getProjectPath() {
    return projectPath;
  }

  @NotEmpty
  public String getSiteKey() {
    return siteKey;
  }

  @NotEmpty
  public String getCredentialConfigurationJson() {
    return credentialConfigurationJson;
  }
}
