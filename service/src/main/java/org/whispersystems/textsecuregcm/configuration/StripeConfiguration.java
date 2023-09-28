/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import java.util.Map;
import java.util.Set;
import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;
import org.whispersystems.textsecuregcm.subscriptions.PaymentMethod;

public record StripeConfiguration(@NotNull SecretString apiKey,
                                  @NotNull SecretBytes idempotencyKeyGenerator,
                                  @NotBlank String boostDescription,
                                  @Valid @NotEmpty Map<PaymentMethod, Set<@NotBlank String>> supportedCurrenciesByPaymentMethod) {
}
