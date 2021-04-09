/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import io.dropwizard.util.DataSize;

public class Constants {
  public static final String METRICS_NAME = "textsecure";
  public static final int MAXIMUM_STICKER_SIZE_BYTES = (int) DataSize.kibibytes(300).toBytes();
  public static final int MAXIMUM_STICKER_MANIFEST_SIZE_BYTES = (int) DataSize.kibibytes(10).toBytes();
}
