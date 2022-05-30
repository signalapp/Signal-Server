package org.whispersystems.textsecuregcm;

public class WhisperServerVersion {
  private static final String VERSION = "${project.version}";

  public static String getServerVersion() {
    return VERSION;
  }

}
