/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.databind.util.StdConverter;
import java.net.URL;

final class URLSerializationConverter extends StdConverter<URL, String> {

  @Override
  public String convert(final URL value) {
    return value.toString();
  }
}
