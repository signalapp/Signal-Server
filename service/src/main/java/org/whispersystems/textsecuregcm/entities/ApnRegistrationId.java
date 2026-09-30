/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.entities;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record ApnRegistrationId(@NotBlank @Size(max = 1024) String apnRegistrationId) {
}
