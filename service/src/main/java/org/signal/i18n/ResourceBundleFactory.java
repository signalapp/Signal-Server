/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.signal.i18n;

import java.util.Locale;
import java.util.ResourceBundle;

public interface ResourceBundleFactory {
  ResourceBundle createBundle(String baseName, Locale locale, ResourceBundle.Control control);
}
