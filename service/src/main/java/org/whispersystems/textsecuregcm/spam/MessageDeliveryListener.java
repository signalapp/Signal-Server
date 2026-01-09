/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.spam;

import org.whispersystems.textsecuregcm.storage.Account;

public interface MessageDeliveryListener {

  void handleMessageDelivered(Account destinationAccount,
      byte destinationDeviceId,
      boolean ephemeral,
      boolean urgent,
      boolean story,
      boolean sealedSender,
      boolean multiRecipient,
      boolean sync);
}
