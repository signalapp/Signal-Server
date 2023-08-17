/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.lifecycle.Managed;
import java.io.FileWriter;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.CommandStopListenerConfiguration;

/**
 * When {@link Managed#stop()} is called, writes to a configured file path
 * <p>
 * Useful for coordinating process termination via a shared file-system, such as a Kubernetes volume mount.
 */
public class CommandStopListener implements Managed {

  private static final Logger logger = LoggerFactory.getLogger(CommandStopListener.class);

  private final String path;

  CommandStopListener(CommandStopListenerConfiguration config) {
    this.path = config.path();
  }

  @Override
  public void start() {

  }

  @Override
  public void stop() {
    try {
      try (FileWriter writer = new FileWriter(path)) {
        writer.write("stopped");
      }
    } catch (final IOException e) {
      logger.error("Failed to open file {}", path, e);
    }
  }
}
