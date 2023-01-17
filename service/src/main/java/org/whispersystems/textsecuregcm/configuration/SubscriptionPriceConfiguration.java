/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import java.math.BigDecimal;
import java.util.Map;
import javax.validation.Valid;
import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.subscriptions.SubscriptionProcessor;

public record SubscriptionPriceConfiguration(@Valid @NotEmpty Map<SubscriptionProcessor, @NotBlank String> processorIds,
                                             @NotNull @DecimalMin("0.01") BigDecimal amount) {

}
