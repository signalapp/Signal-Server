package org.whispersystems.textsecuregcm.util;

public class ObsoletePhoneNumberFormatException extends Exception {

  private final String regionCode;

  public ObsoletePhoneNumberFormatException(final String regionCode) {
    super("The provided format is obsolete in %s".formatted(regionCode));
    this.regionCode = regionCode;
  }

  public String getRegionCode() {
    return regionCode;
  }
}
