package org.whispersystems.textsecuregcm.liquibase;

import com.codahale.metrics.MetricRegistry;
import net.sourceforge.argparse4j.inf.Namespace;

import java.sql.SQLException;

import io.dropwizard.Configuration;
import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.db.DatabaseConfiguration;
import io.dropwizard.db.ManagedDataSource;
import io.dropwizard.db.PooledDataSourceFactory;
import io.dropwizard.setup.Bootstrap;
import liquibase.Liquibase;
import liquibase.exception.LiquibaseException;
import liquibase.exception.ValidationFailedException;

public abstract class AbstractLiquibaseCommand<T extends Configuration> extends ConfiguredCommand<T> {

  private final DatabaseConfiguration<T> strategy;
  private final Class<T>                 configurationClass;
  private final String                   migrations;

  protected AbstractLiquibaseCommand(String name,
                                     String description,
                                     String migrations,
                                     DatabaseConfiguration<T> strategy,
                                     Class<T> configurationClass) {
    super(name, description);
    this.migrations         = migrations;
    this.strategy           = strategy;
    this.configurationClass = configurationClass;
  }

  @Override
  protected Class<T> getConfigurationClass() {
    return configurationClass;
  }

  @Override
  @SuppressWarnings("UseOfSystemOutOrSystemErr")
  protected void run(Bootstrap<T> bootstrap, Namespace namespace, T configuration) throws Exception {
    final PooledDataSourceFactory dbConfig = strategy.getDataSourceFactory(configuration);
    dbConfig.asSingleConnectionPool();

    try (final CloseableLiquibase liquibase = openLiquibase(dbConfig, namespace)) {
      run(namespace, liquibase);
    } catch (ValidationFailedException e) {
      e.printDescriptiveError(System.err);
      throw e;
    }
  }

  private CloseableLiquibase openLiquibase(final PooledDataSourceFactory dataSourceFactory, final Namespace namespace)
      throws ClassNotFoundException, SQLException, LiquibaseException
  {
    final ManagedDataSource dataSource = dataSourceFactory.build(new MetricRegistry(), "liquibase");
    return new CloseableLiquibase(dataSource, migrations);
  }

  protected abstract void run(Namespace namespace, Liquibase liquibase) throws Exception;

}
