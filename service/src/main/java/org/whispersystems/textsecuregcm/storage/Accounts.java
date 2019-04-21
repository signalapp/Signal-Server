/*
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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.whispersystems.textsecuregcm.storage.mappers.AccountRowMapper;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import java.util.List;
import java.util.Optional;

import static com.codahale.metrics.MetricRegistry.name;

public class Accounts {

  public static final String ID     = "id";
  public static final String NUMBER = "number";
  public static final String DATA   = "data";

  private static final ObjectMapper mapper = SystemMapper.getMapper();

  private final MetricRegistry metricRegistry        = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Timer          createTimer           = metricRegistry.timer(name(Accounts.class, "create"));
  private final Timer          updateTimer           = metricRegistry.timer(name(Accounts.class, "update"));
  private final Timer          getTimer              = metricRegistry.timer(name(Accounts.class, "get"));
  private final Timer          getAllFromTimer       = metricRegistry.timer(name(Accounts.class, "getAllFrom"));
  private final Timer          getAllFromOffsetTimer = metricRegistry.timer(name(Accounts.class, "getAllFromOffset"));
  private final Timer          vacuumTimer           = metricRegistry.timer(name(Accounts.class, "vacuum"));

  private final FaultTolerantDatabase database;

  public Accounts(FaultTolerantDatabase database) {
    this.database = database;
    this.database.getDatabase().registerRowMapper(new AccountRowMapper());
  }

  public boolean create(Account account) {
    return database.with(jdbi -> jdbi.inTransaction(TransactionIsolationLevel.SERIALIZABLE, handle -> {
      try (Timer.Context ignored = createTimer.time()) {
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
    }));
  }

  public void update(Account account) {
    database.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context ignored = updateTimer.time()) {
        handle.createUpdate("UPDATE accounts SET " + DATA + " = CAST(:data AS json) WHERE " + NUMBER + " = :number")
              .bind("number", account.getNumber())
              .bind("data", mapper.writeValueAsString(account))
              .execute();
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException(e);
      }
    }));
  }

  public Optional<Account> get(String number) {
    return database.with(jdbi -> jdbi.withHandle(handle -> {
      try (Timer.Context ignored = getTimer.time()) {
        return handle.createQuery("SELECT * FROM accounts WHERE " + NUMBER + " = :number")
                     .bind("number", number)
                     .mapTo(Account.class)
                     .findFirst();
      }
    }));
  }


  public List<Account> getAllFrom(String from, int length) {
    return database.with(jdbi -> jdbi.withHandle(handle -> {
      try (Timer.Context ignored = getAllFromOffsetTimer.time()) {
        return handle.createQuery("SELECT * FROM accounts WHERE " + NUMBER + " > :from ORDER BY " + NUMBER + " LIMIT :limit")
                     .bind("from", from)
                     .bind("limit", length)
                     .mapTo(Account.class)
                     .list();
      }
    }));
  }

  public List<Account> getAllFrom(int length) {
    return database.with(jdbi -> jdbi.withHandle(handle -> {
      try (Timer.Context ignored = getAllFromTimer.time()) {
        return handle.createQuery("SELECT * FROM accounts ORDER BY " + NUMBER + " LIMIT :limit")
                     .bind("limit", length)
                     .mapTo(Account.class)
                     .list();
      }
    }));
  }

  public void vacuum() {
    database.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context ignored = vacuumTimer.time()) {
        handle.execute("VACUUM accounts");
      }
    }));
  }

}
