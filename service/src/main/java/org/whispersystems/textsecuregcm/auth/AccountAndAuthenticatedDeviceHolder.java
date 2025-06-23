/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import java.time.Instant;
import java.util.UUID;

public interface AccountAndAuthenticatedDeviceHolder {

  UUID getAccountIdentifier();

  byte getDeviceId();

  Instant getPrimaryDeviceLastSeen();

  @Deprecated(forRemoval = true)
  Account getAccount();

  @Deprecated(forRemoval = true)
  Device getAuthenticatedDevice();
}
