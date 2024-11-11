/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.Valid;
import jakarta.validation.constraints.DecimalMin;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.util.Map;
import org.whispersystems.textsecuregcm.subscriptions.PaymentProvider;

public record SubscriptionPriceConfiguration(@Valid @NotEmpty Map<PaymentProvider, @NotBlank String> processorIds,
                                             @NotNull @DecimalMin("0.01") BigDecimal amount) {

}
