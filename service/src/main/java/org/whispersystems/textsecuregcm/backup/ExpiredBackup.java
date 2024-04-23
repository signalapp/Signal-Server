/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.backup;

import java.time.Instant;

/**
 * Represents a backup that requires some or all of its content to be deleted
 *
 * @param hashedBackupId The hashedBackupId that owns this content
 * @param expirationType What triggered the expiration
 * @param lastRefresh    The timestamp of the last time the backup user was seen
 * @param prefixToDelete The prefix on the CDN associated with this backup that should be deleted
 */
public record ExpiredBackup(
    byte[] hashedBackupId,
    ExpirationType expirationType,
    Instant lastRefresh,
    String prefixToDelete) {

  public enum ExpirationType {
    // The prefixToDelete expiration is for the entire backup
    ALL,
    // The prefixToDelete is for the media associated with the backup
    MEDIA,
    // The prefixToDelete is from a prior expiration attempt
    GARBAGE_COLLECTION
  }
}
