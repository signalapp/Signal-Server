/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm;

import java.io.File;
import java.util.Arrays;

/**
 * Checks whether all YAML configuration files in a given directory are valid.
 * <p>
 * Note: the current implementation fails fast, rather than reporting multiple invalid files
 */
public class CheckServiceConfigurations {

  private static final String SECRETS_BUNDLE_FILENAME = "sample-secrets-bundle.yml";

  private void checkConfiguration(final File configDirectory) {

    final File[] configFiles = configDirectory.listFiles(f ->
        !f.isDirectory()
            && f.getPath().endsWith(".yml")
            && !f.getPath().endsWith(SECRETS_BUNDLE_FILENAME));

    if (configFiles == null || configFiles.length == 0) {
      throw new IllegalArgumentException("No .yml configuration files found at " + configDirectory.getPath());
    }

    final File[] secretsBundle = configDirectory.listFiles(f -> !f.isDirectory() && f.getName().equals(SECRETS_BUNDLE_FILENAME));
    if (secretsBundle == null || secretsBundle.length != 1) {
      throw new IllegalArgumentException("No [%s] file found at %s".formatted(SECRETS_BUNDLE_FILENAME, configDirectory.getPath()));
    }
    System.setProperty(WhisperServerService.SECRETS_BUNDLE_FILE_NAME_PROPERTY, secretsBundle[0].getAbsolutePath());

    for (final File configFile : configFiles) {
      final String[] args = new String[]{"check", configFile.getAbsolutePath()};
      try {
        new WhisperServerService().run(args);
      } catch (final Exception e) {
        // Invalid configuration will cause the "check" command to call `System.exit()`, rather than throwing,
        // so this is unexpected
        throw new RuntimeException(e);
      }
    }
  }

  public static void main(final String[] args) {
    if (args.length != 1) {
      throw new IllegalArgumentException("Expected single argument with config directory: " + Arrays.toString(args));
    }

    final File configDirectory = new File(args[0]);

    if (!(configDirectory.exists() && configDirectory.isDirectory())) {
      throw new IllegalArgumentException("No directory found at " + configDirectory.getPath());
    }

    new CheckServiceConfigurations().checkConfiguration(configDirectory);
  }
}
