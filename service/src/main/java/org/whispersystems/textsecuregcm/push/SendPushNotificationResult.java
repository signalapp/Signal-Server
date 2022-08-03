/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import javax.annotation.Nullable;

public record SendPushNotificationResult(boolean accepted, @Nullable String errorCode, boolean unregistered) {
}
