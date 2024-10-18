/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Positive;

public record KeyTransparencyServiceConfiguration(@NotBlank String host,
                                                  @Positive int port,
                                                  @NotBlank String tlsCertificate,
                                                  @NotBlank String clientCertificate,
                                                  @NotNull SecretString clientPrivateKey) {}
