/**
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.storage;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.whispersystems.textsecuregcm.storage.mappers.AccountRowMapper;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import java.util.List;
import java.util.Optional;

public class Accounts {

  public static final String ID     = "id";
  public static final String NUMBER = "number";
  public static final String DATA   = "data";

  private static final ObjectMapper mapper = SystemMapper.getMapper();

  private final Jdbi database;

  public Accounts(Jdbi database) {
    this.database = database;
    this.database.registerRowMapper(new AccountRowMapper());
  }

  public boolean create(Account account) {
    return database.inTransaction(TransactionIsolationLevel.SERIALIZABLE, handle -> {
      try {
        int rows = handle.createUpdate("DELETE FROM accounts WHERE " + NUMBER + " = :number")
                         .bind("number", account.getNumber())
                         .execute();

        handle.createUpdate("INSERT INTO accounts (" + NUMBER + ", " + DATA + ") VALUES (:number, CAST(:data AS json))")
              .bind("number", account.getNumber())
              .bind("data", mapper.writeValueAsString(account))
              .execute();


        return rows == 0;
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }
    });
  }

  public void update(Account account) {
    database.useHandle(handle -> {
      try {
        handle.createUpdate("UPDATE accounts SET " + DATA + " = CAST(:data AS json) WHERE " + NUMBER + " = :number")
              .bind("number", account.getNumber())
              .bind("data", mapper.writeValueAsString(account))
              .execute();
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }
    });
  }

  public Optional<Account> get(String number) {
    return database.withHandle(handle -> handle.createQuery("SELECT * FROM accounts WHERE " + NUMBER + " = :number")
                                               .bind("number", number)
                                               .mapTo(Account.class)
                                               .findFirst());
  }


  public List<Account> getAllFrom(String from, int length) {
    return database.withHandle(handle -> handle.createQuery("SELECT * FROM accounts WHERE " + NUMBER + " > :from ORDER BY " + NUMBER + " LIMIT :limit")
                                               .bind("from", from)
                                               .bind("limit", length)
                                               .mapTo(Account.class)
                                               .list());
  }

  public List<Account> getAllFrom(int length) {
    return database.withHandle(handle -> handle.createQuery("SELECT * FROM accounts ORDER BY " + NUMBER + " LIMIT :limit")
                                               .bind("limit", length)
                                               .mapTo(Account.class)
                                               .list());
  }

  public void vacuum() {
    database.useHandle(handle -> handle.execute("VACUUM accounts"));
  }

}
