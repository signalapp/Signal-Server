/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;

public record TestRecipient(ServiceIdentifier uuid,
                            byte[] deviceIds,
                            int[] registrationIds,
                            byte[] perRecipientKeyMaterial) {

  public TestRecipient(ServiceIdentifier uuid,
                       byte deviceId,
                       int registrationId,
                       byte[] perRecipientKeyMaterial) {

    this(uuid, new byte[]{deviceId}, new int[]{registrationId}, perRecipientKeyMaterial);
  }
}
