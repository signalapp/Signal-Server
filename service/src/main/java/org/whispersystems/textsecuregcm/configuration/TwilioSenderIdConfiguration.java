/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.google.common.annotations.VisibleForTesting;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TwilioSenderIdConfiguration {
  @NotEmpty
  private String defaultSenderId;

  @NotNull
  @Valid
  private List<TwilioCountrySenderIdConfiguration> countrySpecificSenderIds = new ArrayList<>();

  @NotNull
  @Valid
  private Set<String> countryCodesWithoutSenderId = new HashSet<>();

  public String getDefaultSenderId() {
    return defaultSenderId;
  }

  @VisibleForTesting
  public void setDefaultSenderId(String defaultSenderId) {
    this.defaultSenderId = defaultSenderId;
  }

  public List<TwilioCountrySenderIdConfiguration> getCountrySpecificSenderIds() {
    return countrySpecificSenderIds;
  }

  @VisibleForTesting
  public void setCountrySpecificSenderIds(List<TwilioCountrySenderIdConfiguration> countrySpecificSenderIds) {
    this.countrySpecificSenderIds = countrySpecificSenderIds;
  }

  public Set<String> getCountryCodesWithoutSenderId() {
    return countryCodesWithoutSenderId;
  }

  @VisibleForTesting
  public void setCountryCodesWithoutSenderId(Set<String> countryCodesWithoutSenderId) {
    this.countryCodesWithoutSenderId = countryCodesWithoutSenderId;
  }
}
