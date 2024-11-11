/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import jakarta.validation.constraints.NotBlank;

public record SubmitVerificationCodeRequest(@NotBlank String code) {

}
