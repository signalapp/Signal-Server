/*
 * Copyright 2021-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import javax.validation.constraints.NotEmpty;

public record RecaptchaConfiguration(@NotEmpty String projectPath, @NotEmpty String credentialConfigurationJson,
                                     @NotEmpty String secondaryCredentialConfigurationJson) {

}
