package org.whispersystems.textsecuregcm.storage.mappers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jdbi.v3.core.mapper.RowMapper;
import org.jdbi.v3.core.statement.StatementContext;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Accounts;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

public class AccountRowMapper implements RowMapper<Account> {

  private static ObjectMapper mapper = SystemMapper.getMapper();

  @Override
  public Account map(ResultSet resultSet, StatementContext ctx) throws SQLException {
    try {
      Account account = mapper.readValue(resultSet.getString(Accounts.DATA), Account.class);
      account.setNumber(resultSet.getString(Accounts.NUMBER));
      account.setUuid(UUID.fromString(resultSet.getString(Accounts.UID)));
      return account;
    } catch (IOException e) {
      throw new SQLException(e);
    }
  }
}