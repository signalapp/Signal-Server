/*
 * Copyright 2013-2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import org.whispersystems.textsecuregcm.util.ExactlySize;

public record DirectoryV2ClientConfiguration(@ExactlySize({32}) byte[] userAuthenticationTokenSharedSecret,
                                             @ExactlySize({32}) byte[] userIdTokenSharedSecret) {
}
