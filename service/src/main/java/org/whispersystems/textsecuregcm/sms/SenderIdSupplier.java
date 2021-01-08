/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.sms;

import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.configuration.TwilioCountrySenderIdConfiguration;
import org.whispersystems.textsecuregcm.configuration.TwilioSenderIdConfiguration;
import org.whispersystems.textsecuregcm.util.Util;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

class SenderIdSupplier {
  private final String              defaultSenderId;
  private final Map<String, String> countrySpecificSenderIds;
  private final Set<String>         countryCodesWithoutSenderId;

  SenderIdSupplier(String defaultSenderId, List<TwilioCountrySenderIdConfiguration> countrySpecificSenderIds, Set<String> countryCodesWithoutSenderId) {
    this.defaultSenderId = defaultSenderId;
    this.countrySpecificSenderIds = countrySpecificSenderIds.stream().collect(Collectors.toMap(
            TwilioCountrySenderIdConfiguration::getCountryCode,
            TwilioCountrySenderIdConfiguration::getSenderId));
    this.countryCodesWithoutSenderId = countryCodesWithoutSenderId;
  }

  SenderIdSupplier(TwilioSenderIdConfiguration configuration) {
    this(configuration.getDefaultSenderId(),
         configuration.getCountrySpecificSenderIds(),
         configuration.getCountryCodesWithoutSenderId());
  }

  Optional<String> get(@NotNull String destination) {
    /* final String countryCode = Util.getCountryCode(destination);
    if (countryCodesWithoutSenderId.contains(countryCode)) {
      return Optional.empty();
    }

    return Optional.ofNullable(StringUtils.stripToNull(countrySpecificSenderIds.getOrDefault(countryCode, defaultSenderId))); */
    return Optional.empty();
  }
}
