/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage.foundationdb;

import com.apple.foundationdb.tuple.Versionstamp;
import java.util.UUID;

class MessageGuidCodec {

  private final UUID accountIdentifier;
  private final byte deviceId;
  private final VersionstampUUIDCipher versionstampUUIDCipher;

  MessageGuidCodec(final UUID accountIdentifier,
      final byte deviceId,
      final VersionstampUUIDCipher versionstampUUIDCipher) {

    this.accountIdentifier = accountIdentifier;
    this.deviceId = deviceId;
    this.versionstampUUIDCipher = versionstampUUIDCipher;
  }

  public UUID encodeMessageGuid(final Versionstamp versionstamp) {
    return versionstampUUIDCipher.encryptVersionstamp(versionstamp, accountIdentifier, deviceId);
  }

  public Versionstamp decodeMessageGuid(final UUID messageGuid) {
    return versionstampUUIDCipher.decryptVersionstamp(messageGuid, accountIdentifier, deviceId);
  }
}
