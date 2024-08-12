/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Positive;

public record KeyTransparencyServiceConfiguration(@NotBlank String host,
                                                  @Positive int port,
                                                  @NotBlank String tlsCertificate) {}
