/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;

public record GroupCredentials(List<GroupCredential> credentials, @Nullable UUID pni) {

  public record GroupCredential(byte[] credential, long redemptionTime) {
  }
}
