/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import java.util.UUID;
import javax.validation.constraints.NotNull;

public record UsernameLinkHandle(@NotNull UUID usernameLinkHandle) {
}
