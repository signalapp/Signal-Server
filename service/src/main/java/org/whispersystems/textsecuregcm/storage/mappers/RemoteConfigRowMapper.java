package org.whispersystems.textsecuregcm.storage.mappers;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.whispersystems.textsecuregcm.storage.RemoteConfig;
import org.whispersystems.textsecuregcm.storage.RemoteConfigs;

import java.sql.ResultSet;
import java.sql.SQLException;


public class RemoteConfigRowMapper implements RowMapper<RemoteConfig> {

  @Override
  public RemoteConfig map(ResultSet rs, StatementContext ctx) throws SQLException {
    return new RemoteConfig(rs.getString(RemoteConfigs.NAME), rs.getInt(RemoteConfigs.PERCENTAGE));
  }
}
