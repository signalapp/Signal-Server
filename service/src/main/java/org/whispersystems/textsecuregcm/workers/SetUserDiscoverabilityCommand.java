/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Environment;
import java.util.Optional;
import java.util.UUID;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;

public class SetUserDiscoverabilityCommand extends AbstractCommandWithDependencies {

  public SetUserDiscoverabilityCommand() {

    super(new Application<>() {
      @Override
      public void run(final WhisperServerConfiguration whisperServerConfiguration, final Environment environment) {
      }
    }, "set-discoverability", "sets whether a user should be discoverable in CDS");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("-u", "--user")
        .dest("user")
        .type(String.class)
        .required(true)
        .help("the user (UUID or E164) for whom to change discoverability");

    subparser.addArgument("-d", "--discoverable")
        .dest("discoverable")
        .type(Boolean.class)
        .required(true)
        .help("whether the user should be discoverable in CDS");
  }

  @Override
  protected void run(final Environment environment,
      final Namespace namespace,
      final WhisperServerConfiguration configuration,
      final CommandDependencies deps) throws Exception {

    try {
      final AccountsManager accountsManager = deps.accountsManager();
      Optional<Account> maybeAccount;

      try {
        maybeAccount = accountsManager.getByAccountIdentifier(UUID.fromString(namespace.getString("user")));
      } catch (final IllegalArgumentException e) {
        maybeAccount = accountsManager.getByE164(namespace.getString("user"));
      }

      maybeAccount.ifPresentOrElse(account -> {
            final boolean initiallyDiscoverable = account.isDiscoverableByPhoneNumber();
            accountsManager.update(account, a -> a.setDiscoverableByPhoneNumber(namespace.getBoolean("discoverable")));

            System.out.format("Set discoverability flag for %s to %s (was previously %s)\n",
                namespace.getString("user"),
                namespace.getBoolean("discoverable"),
                initiallyDiscoverable);
          },
          () -> System.err.println("User not found: " + namespace.getString("user")));
    } catch (final Exception e) {
      System.err.println("Failed to update discoverability setting for " + namespace.getString("user"));
      e.printStackTrace();
    }
  }
}
