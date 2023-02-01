/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import java.util.UUID;
import javax.annotation.Nullable;

public record AccountIdentityResponse(UUID uuid,
                                      String number,
                                      UUID pni,
                                      @Nullable String username,
                                      boolean storageCapable) {
}
