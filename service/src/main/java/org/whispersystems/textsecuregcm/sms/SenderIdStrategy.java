package org.whispersystems.textsecuregcm.sms;

import com.google.common.base.Strings;
import org.whispersystems.textsecuregcm.configuration.TwilioCountrySenderIdConfiguration;
import org.whispersystems.textsecuregcm.configuration.TwilioSenderIdConfiguration;
import org.whispersystems.textsecuregcm.util.Util;

import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SenderIdStrategy {
  private final String              defaultSenderId;
  private final Map<String, String> countrySpecificSenderIds;
  private final Set<String>         countryCodesWithoutSenderId;

  public SenderIdStrategy(String defaultSenderId, List<TwilioCountrySenderIdConfiguration> countrySpecificSenderIds, Set<String> countryCodesWithoutSenderId) {
    this.defaultSenderId = defaultSenderId;
    this.countrySpecificSenderIds = countrySpecificSenderIds.stream().collect(Collectors.toMap(
            TwilioCountrySenderIdConfiguration::getCountryCode,
            TwilioCountrySenderIdConfiguration::getSenderId));
    this.countryCodesWithoutSenderId = countryCodesWithoutSenderId;
  }

  public SenderIdStrategy(TwilioSenderIdConfiguration configuration) {
    this(configuration.getDefaultSenderId(),
         configuration.getCountrySpecificSenderIds(),
         configuration.getCountryCodesWithoutSenderId());
  }

  public Optional<String> get(@NotNull String destination) {
    final String countryCode = Util.getCountryCode(destination);
    if (countryCodesWithoutSenderId.contains(countryCode)) {
      return Optional.empty();
    }

    final String countrySpecificSenderId = countrySpecificSenderIds.get(countryCode);
    if (!Strings.isNullOrEmpty(countrySpecificSenderId)) {
      return Optional.of(countrySpecificSenderId);
    }

    return Optional.ofNullable(defaultSenderId);
  }
}
