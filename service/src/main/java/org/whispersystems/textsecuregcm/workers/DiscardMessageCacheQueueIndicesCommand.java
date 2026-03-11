/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;

public class DiscardMessageCacheQueueIndicesCommand extends AbstractCommandWithDependencies {

  private static final Logger logger = LoggerFactory.getLogger(DiscardMessageCacheQueueIndicesCommand.class);

  public DiscardMessageCacheQueueIndicesCommand() {
    super(new Application<>() {
      @Override
      public void run(final WhisperServerConfiguration configuration, final Environment environment) {
      }
    }, "discard-message-cache-queue-indices", "Removes now-unused message cache queue indices");
  }

  @Override
  protected void run(final Environment environment,
      final Namespace namespace,
      final WhisperServerConfiguration configuration,
      final CommandDependencies commandDependencies) throws Exception {

    commandDependencies.messagesCache().discardQueueIndices();
    logger.info("Finished discarding message cache queue indices");
  }
}
