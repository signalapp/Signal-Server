/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth.grpc;

import java.util.UUID;

public record AuthenticatedDevice(UUID accountIdentifier, byte deviceId) {
}
