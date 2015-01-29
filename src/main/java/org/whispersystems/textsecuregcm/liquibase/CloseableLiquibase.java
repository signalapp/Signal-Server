package org.whispersystems.textsecuregcm.liquibase;

import java.sql.SQLException;

import io.dropwizard.db.ManagedDataSource;
import liquibase.Liquibase;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;


public class CloseableLiquibase extends Liquibase implements AutoCloseable {
  private final ManagedDataSource dataSource;

  public CloseableLiquibase(ManagedDataSource dataSource, String migrations)
      throws LiquibaseException, ClassNotFoundException, SQLException
  {
    super(migrations,
          new ClassLoaderResourceAccessor(),
          new JdbcConnection(dataSource.getConnection()));
    this.dataSource = dataSource;
  }

  @Override
  public void close() throws Exception {
    dataSource.stop();
  }
}
