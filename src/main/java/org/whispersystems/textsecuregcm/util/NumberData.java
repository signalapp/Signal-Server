package org.whispersystems.textsecuregcm.util;

public class NumberData {
  private String number;
  private boolean active;
  private boolean supportsSms;

  public NumberData(String number, boolean active, boolean supportsSms) {
    this.number = number;
    this.active = active;
    this.supportsSms = supportsSms;
  }

  public boolean isActive() {
    return active;
  }

  public boolean isSupportsSms() {
    return supportsSms;
  }

  public String getNumber() {
    return number;
  }
}
