/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.cli.Command;
import io.dropwizard.setup.Bootstrap;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import net.sourceforge.argparse4j.inf.ArgumentAction;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
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

    subparser.addArgument("-c", "--class")
        .type(String.class)
        .nargs("*")
        .setDefault(DynamicConfiguration.class.getCanonicalName());
  }

  @Override
  public void run(final Bootstrap<?> bootstrap, final Namespace namespace) throws Exception {
    final Path path = Path.of(namespace.getString("file"));

    final List<Class<?>> configurationClasses;

    if (namespace.get("class") instanceof List) {
      final List<Class<?>> classesFromArguments = new ArrayList<>();

      for (final Object object : namespace.getList("class")) {
        classesFromArguments.add(Class.forName(object.toString()));
      }

      configurationClasses = classesFromArguments;
    } else {
      configurationClasses = List.of(Class.forName(namespace.getString("class")));
    }

    for (final Class<?> configurationClass : configurationClasses) {
      if (DynamicConfigurationManager.parseConfiguration(Files.readString(path), configurationClass).isPresent()) {
        System.out.println(configurationClass.getSimpleName() + ": dynamic configuration file at " + path + " is valid");
      } else {
        System.err.println(configurationClass.getSimpleName() + ": dynamic configuration file at " + path + " is not valid");
      }
    }
  }
}
