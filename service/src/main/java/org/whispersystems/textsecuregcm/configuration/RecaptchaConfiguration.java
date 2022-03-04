/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import javax.validation.constraints.NotEmpty;

public class RecaptchaConfiguration {

  private String projectPath;
  private String credentialConfigurationJson;

  @NotEmpty
  public String getProjectPath() {
    return projectPath;
  }

  @NotEmpty
  public String getCredentialConfigurationJson() {
    return credentialConfigurationJson;
  }
}
