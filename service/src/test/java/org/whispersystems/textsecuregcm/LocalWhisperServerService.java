/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm;

import io.dropwizard.util.Resources;

/**
 * This class may be run directly from a correctly configured IDE, or using the command line:
 * <p>
 * <code>./mvnw clean integration-test -DskipTests=true -Ptest-server</code>
 * <p>
 * <strong>NOTE: many features are non-functional, especially those that depend on external services</strong>
 */
public class LocalWhisperServerService {

  public static void main(String[] args) throws Exception {

    System.setProperty("secrets.bundle.filename",
        Resources.getResource("config/test-secrets-bundle.yml").getPath());
    System.setProperty("sqlite.dir", "service/target/lib");
    System.setProperty("aws.region", "local-test-region");

    new WhisperServerService().run("server", Resources.getResource("config/test.yml").getPath());
  }
}
