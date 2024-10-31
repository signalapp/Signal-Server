/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.core.cli.Command;
import io.dropwizard.core.setup.Bootstrap;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
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

  private boolean isValid(final Class<?> configurationClass, final String yamlConfig) {
    return DynamicConfigurationManager.parseConfiguration(yamlConfig, configurationClass).isPresent();
  }

  /**
   * Throw to exit the command cleanly but with a non-zero exit code
   */
  private static class CommandFailedException extends RuntimeException {
    @Override
    public synchronized Throwable fillInStackTrace() {
      return this;
    }
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

    final String yamlConfig = Files.readString(path);
    final boolean allValid = configurationClasses.stream()
        .allMatch(cls -> {
          final boolean valid = isValid(cls, yamlConfig);
          if (valid) {
            System.out.println(cls.getSimpleName() + ": dynamic configuration file at " + path + " is valid");
          } else {
            System.err.println(cls.getSimpleName() + ": dynamic configuration file at " + path + " is not valid");
          }
          return valid;
        });

    if (!allValid) {
      throw new CommandFailedException();
    }
  }
}
