/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.badges;

import java.util.List;
import java.util.Locale;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.storage.AccountBadge;

public interface ProfileBadgeConverter {

  /// Determine displayable badge ids for an account's badges
  ///
  /// @param badges The full list of badges on an account
  /// @return The list of badge-ids that should appear on this account's profile to other users
  List<String> visibleBadgeIds(final List<AccountBadge> badges);

  /**
   * Converts the {@link AccountBadge}s for an account into the objects
   * that can be returned on a profile fetch.
   */
  List<Badge> convert(List<Locale> acceptableLanguages, List<AccountBadge> accountBadges, boolean isSelf);
}
