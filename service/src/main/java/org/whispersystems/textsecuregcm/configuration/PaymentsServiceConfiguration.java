/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import java.util.List;

public record PaymentsServiceConfiguration(@NotNull SecretBytes userAuthenticationTokenSharedSecret,
                                           @NotEmpty List<String> paymentCurrencies,
                                           @NotNull @Valid PaymentsServiceClientsFactory externalClients) {


}
