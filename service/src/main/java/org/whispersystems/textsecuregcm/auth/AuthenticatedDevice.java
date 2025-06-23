/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.security.Principal;
import java.time.Instant;
import java.util.UUID;
import javax.security.auth.Subject;

public record AuthenticatedDevice(UUID accountIdentifier, byte deviceId, Instant primaryDeviceLastSeen)
    implements Principal {

  @Override
  public String getName() {
    return null;
  }

  @Override
  public boolean implies(final Subject subject) {
    return false;
  }
}
