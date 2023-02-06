/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;

public record RegistrationRequest(@NotBlank String sessionId,
                                  @NotNull @Valid AccountAttributes accountAttributes,
                                  boolean skipDeviceTransfer) {

}
