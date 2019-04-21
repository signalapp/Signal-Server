package org.whispersystems.textsecuregcm.liquibase;

import com.google.common.collect.Maps;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.util.SortedMap;

import io.dropwizard.Configuration;
import io.dropwizard.db.DatabaseConfiguration;
import liquibase.Liquibase;

public class NameableDbCommand<T extends Configuration> extends AbstractLiquibaseCommand<T> {
  private static final String COMMAND_NAME_ATTR = "subcommand";
  private final SortedMap<String, AbstractLiquibaseCommand<T>> subcommands;

  public NameableDbCommand(String name, String migrations, DatabaseConfiguration<T> strategy, Class<T> configurationClass) {
    super(name, "Run database migrations tasks", migrations, strategy, configurationClass);
    this.subcommands = Maps.newTreeMap();
    addSubcommand(new DbMigrateCommand<>(migrations, strategy, configurationClass));
    addSubcommand(new DbStatusCommand<>(migrations, strategy, configurationClass));
  }

  private void addSubcommand(AbstractLiquibaseCommand<T> subcommand) {
    subcommands.put(subcommand.getName(), subcommand);
  }

  @Override
  public void configure(Subparser subparser) {
    for (AbstractLiquibaseCommand<T> subcommand : subcommands.values()) {
      final Subparser cmdParser = subparser.addSubparsers()
                                           .addParser(subcommand.getName())
                                           .setDefault(COMMAND_NAME_ATTR, subcommand.getName())
                                           .description(subcommand.getDescription());
      subcommand.configure(cmdParser);
    }
  }

  @Override
  public void run(Namespace namespace, Liquibase liquibase) throws Exception {
    final AbstractLiquibaseCommand<T> subcommand = subcommands.get(namespace.getString(COMMAND_NAME_ATTR));
    subcommand.run(namespace, liquibase);
  }
}
