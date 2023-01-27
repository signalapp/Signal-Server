/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import org.whispersystems.textsecuregcm.util.ExactlySize;

public record SecureValueRecovery2Configuration(
    @ExactlySize({32}) byte[] userAuthenticationTokenSharedSecret,
    @ExactlySize({32}) byte[] userIdTokenSharedSecret) {
}
