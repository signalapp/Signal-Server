/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;

public record KeyTransparencyServiceConfiguration(@NotBlank String host,
                                                  @Positive int port,
                                                  @NotBlank String tlsCertificate,
                                                  @NotBlank String clientCertificate,
                                                  @NotNull SecretString clientPrivateKey) {}
