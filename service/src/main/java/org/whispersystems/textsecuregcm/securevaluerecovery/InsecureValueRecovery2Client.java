/*
 * Copyright 2023 Signal Messenger, LLC
 * Copyright 2025 Molly Instant Messenger
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.securevaluerecovery;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class InsecureValueRecovery2Client implements ValueRecovery2Client {
  public InsecureValueRecovery2Client(){};

  public CompletableFuture<Void> deleteBackups(final UUID accountUuid) {
    return null;
  }
}
