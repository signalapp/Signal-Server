package org.whispersystems.textsecuregcm.util;

import io.dropwizard.util.DataSize;

public class Constants {
  public static final String METRICS_NAME = "textsecure";
  public static final int MAXIMUM_STICKER_SIZE_BYTES = (int) DataSize.kibibytes(300).toBytes();
  public static final int MAXIMUM_STICKER_MANIFEST_SIZE_BYTES = (int) DataSize.kibibytes(10).toBytes();
}
