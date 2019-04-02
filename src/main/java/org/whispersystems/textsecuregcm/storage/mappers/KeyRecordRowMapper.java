package org.whispersystems.textsecuregcm.storage.mappers;

import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.whispersystems.textsecuregcm.storage.KeyRecord;

import java.sql.ResultSet;
import java.sql.SQLException;

public class KeyRecordRowMapper implements RowMapper<KeyRecord> {

  @Override
  public KeyRecord map(ResultSet resultSet, StatementContext ctx) throws SQLException {
    return new KeyRecord(resultSet.getLong("id"),
                         resultSet.getString("number"),
                         resultSet.getLong("device_id"),
                         resultSet.getLong("key_id"),
                         resultSet.getString("public_key"));
  }
}
