/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import javax.validation.constraints.NotBlank;

public record HCaptchaConfiguration(@NotBlank String apiKey) {
}
