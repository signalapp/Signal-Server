/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import com.codahale.metrics.jdbi3.strategies.DefaultNameStrategy;
import io.dropwizard.Application;
import io.dropwizard.cli.EnvironmentCommand;
import io.dropwizard.jdbi3.JdbiFactory;
import io.dropwizard.setup.Environment;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.AbusiveHostRules;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import java.util.concurrent.atomic.AtomicInteger;

public class MigrateAbusiveHostRulesCommand extends EnvironmentCommand<WhisperServerConfiguration> {

  private static final Logger log = LoggerFactory.getLogger(MigrateAbusiveHostRulesCommand.class);

  public MigrateAbusiveHostRulesCommand() {
    super(new Application<>() {
      @Override
      public void run(WhisperServerConfiguration configuration, Environment environment) {
      }
    }, "migrate-abusive-host-rules", "Migrate abusive host rules from one Postgres to another");
  }

  @Override
  public void configure(Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("-s", "--fetch-size")
        .dest("fetchSize")
        .type(Integer.class)
        .required(false)
        .setDefault(512)
        .help("The number of rules to fetch from Postgres at once");
  }

  @Override
  protected void run(final Environment environment, final Namespace namespace,
      final WhisperServerConfiguration config) throws Exception {

    DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        new DynamicConfigurationManager<>(config.getAppConfig().getApplication(),
            config.getAppConfig().getEnvironment(),
            config.getAppConfig().getConfigurationName(),
            DynamicConfiguration.class);

    JdbiFactory jdbiFactory = new JdbiFactory(DefaultNameStrategy.CHECK_EMPTY);
    Jdbi abuseJdbi = jdbiFactory.build(environment, config.getAbuseDatabaseConfiguration(), "abusedb");

    FaultTolerantDatabase abuseDatabase = new FaultTolerantDatabase("abuse_database", abuseJdbi,
        config.getAbuseDatabaseConfiguration().getCircuitBreakerConfiguration());

    Jdbi newAbuseJdbi = jdbiFactory.build(environment, config.getNewAbuseDatabaseConfiguration(), "abusedb2");
    FaultTolerantDatabase newAbuseDatabase = new FaultTolerantDatabase("abuse_database2", newAbuseJdbi,
        config.getNewAbuseDatabaseConfiguration().getCircuitBreakerConfiguration());

    log.info("Beginning migration");

    AbusiveHostRules abusiveHostRules = new AbusiveHostRules(abuseDatabase, newAbuseDatabase,
        dynamicConfigurationManager);

    final int fetchSize = namespace.getInt("fetchSize");

    final AtomicInteger rulesMigrated = new AtomicInteger(0);

    abusiveHostRules.forEachInOldDatabase((rule, notes) -> {

      abusiveHostRules.migrateAbusiveHostRule(rule, notes);

      int migrated = rulesMigrated.incrementAndGet();

      if (migrated % 1_000 == 0) {
        log.info("Migrated {} rules", migrated);
      }
    }, fetchSize);

    log.info("Migration complete ({} total rules)", rulesMigrated.get());
  }

}
