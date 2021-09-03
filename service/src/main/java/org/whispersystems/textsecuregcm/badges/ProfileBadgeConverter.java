/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.badges;

import java.util.Set;
import javax.ws.rs.core.Request;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.storage.AccountBadge;

public interface ProfileBadgeConverter {

  /**
   * Converts the {@link AccountBadge}s for an account into the objects
   * that can be returned on a profile fetch. This requires returning
   * localized strings given the user's locale which will be retrieved from
   * the {@link Request} object passed in.
   */
  Set<Badge> convert(Request request, Set<AccountBadge> accountBadges);
}
