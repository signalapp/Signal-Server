/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import java.util.List;
import java.util.Map;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;

public record PaymentsServiceConfiguration(@NotNull SecretBytes userAuthenticationTokenSharedSecret,
                                           @NotNull SecretString coinMarketCapApiKey,
                                           @NotNull SecretString fixerApiKey,
                                           @NotEmpty Map<@NotBlank String, Integer> coinMarketCapCurrencyIds,
                                           @NotEmpty List<String> paymentCurrencies) {
}
