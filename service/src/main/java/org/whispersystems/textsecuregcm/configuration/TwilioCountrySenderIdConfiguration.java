/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.google.common.annotations.VisibleForTesting;

import javax.validation.constraints.NotEmpty;

public class TwilioCountrySenderIdConfiguration {
  @NotEmpty
  private String countryCode;

  @NotEmpty
  private String senderId;

  public String getCountryCode() {
    return countryCode;
  }

  @VisibleForTesting
  public void setCountryCode(String countryCode) {
    this.countryCode = countryCode;
  }

  public String getSenderId() {
    return senderId;
  }

  @VisibleForTesting
  public void setSenderId(String senderId) {
    this.senderId = senderId;
  }
}
