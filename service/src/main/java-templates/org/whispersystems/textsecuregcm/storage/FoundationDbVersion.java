/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

public class FoundationDbVersion {

  private static final String VERSION = "${foundationdb.version}";
  private static final int API_VERSION = ${foundationdb.api-version};

  public static String getFoundationDbVersion() {
    return VERSION;
  }

  public static int getFoundationDbApiVersion() {
    return API_VERSION;
  }
}
