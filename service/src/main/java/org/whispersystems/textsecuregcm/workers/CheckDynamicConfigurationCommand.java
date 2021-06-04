/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.cli.Command;
import io.dropwizard.setup.Bootstrap;
import java.nio.file.Files;
import java.nio.file.Path;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public class CheckDynamicConfigurationCommand extends Command {

  public CheckDynamicConfigurationCommand() {
    super("check-dynamic-config", "Check validity of a dynamic configuration file");
  }

  @Override
  public void configure(final Subparser subparser) {
    subparser.addArgument("file")
        .type(String.class)
        .required(true)
        .help("Dynamic configuration file to check");
  }

  @Override
  public void run(final Bootstrap<?> bootstrap, final Namespace namespace) throws Exception {
    final Path path = Path.of(namespace.getString("file"));

    if (DynamicConfigurationManager.parseConfiguration(Files.readString(path)).isPresent()) {
      System.out.println("Dynamic configuration file at " + path + " is valid");
    } else {
      System.err.println("Dynamic configuration file at " + path + " is not valid");
    }
  }
}
