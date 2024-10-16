/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.time.Instant;
import java.util.Objects;

public record AccountBadge(String id, Instant expiration, boolean visible) {

  /**
   * Returns a new AccountBadge that is a merging of the two originals. IDs must match for this operation to make sense.
   * The expiration will be the later of the two. Visibility will be set if either of the passed in objects is visible.
   */
  public AccountBadge mergeWith(AccountBadge other) {
    if (!Objects.equals(other.id, id)) {
      throw new IllegalArgumentException("merging badges should only take place for same id");
    }

    final Instant latestExpiration;
    if (expiration == null || other.expiration == null) {
      latestExpiration = null;
    } else if (expiration.isAfter(other.expiration)) {
      latestExpiration = expiration;
    } else {
      latestExpiration = other.expiration;
    }

    return new AccountBadge(
        id,
        latestExpiration,
        visible || other.visible
    );
  }

  public AccountBadge withVisibility(boolean visible) {
    if (this.visible == visible) {
      return this;
    } else {
      return new AccountBadge(
          this.id,
          this.expiration,
          visible);
    }
  }
}
