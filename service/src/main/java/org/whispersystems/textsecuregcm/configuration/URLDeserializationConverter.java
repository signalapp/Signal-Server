/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.databind.util.StdConverter;
import java.net.MalformedURLException;
import java.net.URL;

final class URLDeserializationConverter extends StdConverter<String, URL> {

  @Override
  public URL convert(final String value) {
    try {
      return new URL(value);
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
