/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import org.signal.libsignal.zkgroup.backups.BackupCredentialType;
import org.signal.libsignal.zkgroup.backups.BackupLevel;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import javax.annotation.Nullable;

public record AuthenticatedBackupUser(
    byte[] backupId,
    BackupCredentialType credentialType,
    BackupLevel backupLevel,
    String backupDir,
    String mediaDir,
    @Nullable UserAgent userAgent) {
}
