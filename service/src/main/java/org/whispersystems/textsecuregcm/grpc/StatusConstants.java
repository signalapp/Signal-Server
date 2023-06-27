/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Status;

public abstract class StatusConstants {
  public static final Status UPGRADE_NEEDED_STATUS = Status.INVALID_ARGUMENT.withDescription("signal-upgrade-required");
}
