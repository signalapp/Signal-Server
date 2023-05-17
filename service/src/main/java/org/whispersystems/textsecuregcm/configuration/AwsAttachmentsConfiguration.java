/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;

public record AwsAttachmentsConfiguration(@NotNull SecretString accessKey,
                                          @NotNull SecretString accessSecret,
                                          @NotBlank String bucket,
                                          @NotBlank String region) {
}
