/*
 * Copyright 2023 Signal Messenger, LLC
 * Copyright 2025 Molly Instant Messenger
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.securestorage;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

public class InsecureStorageClient implements StorageClient {
  public InsecureStorageClient() {  }

  public CompletableFuture<Void> deleteStoredData(final UUID accountUuid) {
    return null;
  }
}
