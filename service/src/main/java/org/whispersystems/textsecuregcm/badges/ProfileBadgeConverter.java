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

  /**
   * Converts the {@link AccountBadge}s for an account into the objects
   * that can be returned on a profile fetch.
   */
  List<Badge> convert(List<Locale> acceptableLanguages, List<AccountBadge> accountBadges, boolean isSelf);
}
