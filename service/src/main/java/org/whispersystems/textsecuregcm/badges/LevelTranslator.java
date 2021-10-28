/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.badges;

import java.util.List;
import java.util.Locale;

public interface LevelTranslator {
  String translate(List<Locale> acceptableLanguages, String badgeId);
}
