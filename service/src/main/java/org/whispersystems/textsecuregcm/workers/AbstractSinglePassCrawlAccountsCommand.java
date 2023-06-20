/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.util.logging.UncaughtExceptionHandler;
import reactor.core.publisher.Flux;
import java.util.Objects;

public abstract class AbstractSinglePassCrawlAccountsCommand extends EnvironmentCommand<WhisperServerConfiguration> {

  private CommandDependencies commandDependencies;

  private static final String SEGMENT_COUNT = "segments";

  public AbstractSinglePassCrawlAccountsCommand(final String name, final String description) {
    super(new Application<>() {
      @Override
      public void run(final WhisperServerConfiguration configuration, final Environment environment) {
      }
    }, name, description);
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("--segments")
        .type(Integer.class)
        .dest(SEGMENT_COUNT)
        .required(false)
        .setDefault(1)
        .help("The total number of segments for a DynamoDB scan");
  }

  protected CommandDependencies getCommandDependencies() {
    return commandDependencies;
  }

  @Override
  protected void run(final Environment environment, final Namespace namespace,
      final WhisperServerConfiguration configuration) throws Exception {

    UncaughtExceptionHandler.register();

    MetricsUtil.configureRegistries(configuration, environment);
    commandDependencies = CommandDependencies.build(getName(), environment, configuration);

    final int segments = Objects.requireNonNull(namespace.getInt(SEGMENT_COUNT));
    crawlAccounts(commandDependencies.accountsManager().streamAllFromDynamo(segments));
  }

  protected abstract void crawlAccounts(final Flux<Account> accounts);
}
