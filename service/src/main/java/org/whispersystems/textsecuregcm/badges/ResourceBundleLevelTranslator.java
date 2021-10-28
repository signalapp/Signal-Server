/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.badges;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.ResourceBundle;
import javax.annotation.Nonnull;
import org.signal.i18n.HeaderControlledResourceBundleLookup;

public class ResourceBundleLevelTranslator implements LevelTranslator {

  private static final String BASE_NAME = "org.signal.subscriptions.Subscriptions";

  private final HeaderControlledResourceBundleLookup headerControlledResourceBundleLookup;

  public ResourceBundleLevelTranslator(
      @Nonnull final HeaderControlledResourceBundleLookup headerControlledResourceBundleLookup) {
    this.headerControlledResourceBundleLookup = Objects.requireNonNull(headerControlledResourceBundleLookup);
  }

  @Override
  public String translate(final List<Locale> acceptableLanguages, final String badgeId) {
    final ResourceBundle resourceBundle = headerControlledResourceBundleLookup.getResourceBundle(BASE_NAME,
        acceptableLanguages);
    return resourceBundle.getString(badgeId);
  }
}
