/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import java.time.Instant;
import java.util.Optional;

public record SendPushNotificationResult(boolean accepted,
                                         Optional<String> errorCode,
                                         boolean unregistered,
                                         Optional<Instant> unregisteredTimestamp) {
}
