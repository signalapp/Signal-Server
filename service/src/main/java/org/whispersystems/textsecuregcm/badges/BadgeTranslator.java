/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.badges;

import java.util.List;
import java.util.Locale;
import org.whispersystems.textsecuregcm.entities.Badge;

public interface BadgeTranslator {
  Badge translate(List<Locale> acceptableLanguages, String badgeId);
}
