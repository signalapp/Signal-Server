/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import org.whispersystems.textsecuregcm.backup.BackupTier;

public record AuthenticatedBackupUser(byte[] backupId, BackupTier backupTier) {}
