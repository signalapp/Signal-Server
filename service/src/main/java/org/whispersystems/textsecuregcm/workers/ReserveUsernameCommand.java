/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.storage.ReservedUsernames;
import org.whispersystems.textsecuregcm.util.DynamoDbFromConfig;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import java.util.UUID;
import java.util.regex.Pattern;

public class ReserveUsernameCommand extends ConfiguredCommand<WhisperServerConfiguration> {

  public ReserveUsernameCommand() {
    super("reserve-username", "Reserve a username pattern for a specific account identifier");
  }

  @Override
  public void configure(final Subparser subparser) {
    super.configure(subparser);

    subparser.addArgument("-p", "--pattern")
        .dest("pattern")
        .type(String.class)
        .required(true);

    subparser.addArgument("-u", "--uuid")
        .dest("uuid")
        .type(String.class)
        .required(true);
  }

  @Override
  protected void run(final Bootstrap<WhisperServerConfiguration> bootstrap, final Namespace namespace,
      final WhisperServerConfiguration config) throws Exception {

    final DynamoDbClient dynamoDbClient = DynamoDbFromConfig.client(config.getDynamoDbClientConfiguration(),
        software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider.create());

    final ReservedUsernames reservedUsernames = new ReservedUsernames(dynamoDbClient,
        config.getDynamoDbTables().getReservedUsernames().getTableName());

    final String pattern = namespace.getString("pattern").trim();

    try {
      Pattern.compile(pattern);
    } catch (final Exception e) {
      throw new IllegalArgumentException("Bad pattern: " + pattern, e);
    }

    final UUID aci = UUID.fromString(namespace.getString("uuid").trim());

    reservedUsernames.reserveUsername(pattern, aci);

    System.out.format("Reserved %s for account %s\n", pattern, aci);
  }
}
