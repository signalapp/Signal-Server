/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import java.util.Set;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;

public record StripeConfiguration(@NotBlank String apiKey,
                                  @NotEmpty byte[] idempotencyKeyGenerator,
                                  @NotBlank String boostDescription,
                                  @NotEmpty Set<@NotBlank String> supportedCurrencies) {

}
