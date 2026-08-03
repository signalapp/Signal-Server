/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import java.util.Optional;
import java.util.UUID;

public record LinkDeviceResponse(UUID uuid, Optional<UUID> pni, byte deviceId) {
}
