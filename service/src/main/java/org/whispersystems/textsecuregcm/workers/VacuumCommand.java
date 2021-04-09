/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import net.sourceforge.argparse4j.inf.Namespace;
import org.jdbi.v3.core.Jdbi;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.configuration.DatabaseConfiguration;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.storage.PendingAccounts;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;


public class VacuumCommand extends ConfiguredCommand<WhisperServerConfiguration> {

  private final Logger logger = LoggerFactory.getLogger(VacuumCommand.class);

  public VacuumCommand() {
    super("vacuum", "Vacuum Postgres Tables");
  }

  @Override
  protected void run(Bootstrap<WhisperServerConfiguration> bootstrap,
                     Namespace namespace,
                     WhisperServerConfiguration config)
      throws Exception
  {
    DatabaseConfiguration accountDbConfig = config.getAbuseDatabaseConfiguration();
    Jdbi accountJdbi = Jdbi.create(accountDbConfig.getUrl(), accountDbConfig.getUser(), accountDbConfig.getPassword());
    FaultTolerantDatabase accountDatabase = new FaultTolerantDatabase("account_database_vacuum", accountJdbi, accountDbConfig.getCircuitBreakerConfiguration());

    Accounts        accounts        = new Accounts(accountDatabase);
    PendingAccounts pendingAccounts = new PendingAccounts(accountDatabase);

    logger.info("Vacuuming accounts...");
    accounts.vacuum();

    logger.info("Vacuuming pending_accounts...");
    pendingAccounts.vacuum();

    Thread.sleep(3000);
    System.exit(0);
  }
}
