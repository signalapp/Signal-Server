/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import java.util.List;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;

public record PaymentsServiceConfiguration(@NotNull SecretBytes userAuthenticationTokenSharedSecret,
                                           @NotEmpty List<String> paymentCurrencies,
                                           @NotNull @Valid PaymentsServiceClientsFactory externalClients) {


}
