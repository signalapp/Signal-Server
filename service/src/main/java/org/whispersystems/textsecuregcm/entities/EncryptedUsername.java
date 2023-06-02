/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;

public record EncryptedUsername(@NotNull @Size(min = 1, max = 128) byte[] usernameLinkEncryptedValue) {
}
