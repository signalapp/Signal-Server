/*
 * Copyright (C) 2014 Open WhisperSystems
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
import org.whispersystems.textsecuregcm.auth.StoredVerificationCode;
import org.whispersystems.textsecuregcm.storage.mappers.StoredVerificationCodeRowMapper;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.Optional;

import static com.codahale.metrics.MetricRegistry.name;

public class PendingDevices {

  private final MetricRegistry metricRegistry        = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Timer          insertTimer           = metricRegistry.timer(name(PendingDevices.class, "insert"          ));
  private final Timer          getCodeForNumberTimer = metricRegistry.timer(name(PendingDevices.class, "getcodeForNumber"));
  private final Timer          removeTimer           = metricRegistry.timer(name(PendingDevices.class, "remove"          ));

  private final FaultTolerantDatabase database;

  public PendingDevices(FaultTolerantDatabase database) {
    this.database = database;
    this.database.getDatabase().registerRowMapper(new StoredVerificationCodeRowMapper());
  }

  public void insert(String number, String verificationCode, long timestamp) {
    database.use(jdbi ->jdbi.useHandle(handle -> {
      try (Timer.Context timer = insertTimer.time()) {
        handle.createUpdate("WITH upsert AS (UPDATE pending_devices SET verification_code = :verification_code, timestamp = :timestamp WHERE number = :number RETURNING *) " +
                                "INSERT INTO pending_devices (number, verification_code, timestamp) SELECT :number, :verification_code, :timestamp WHERE NOT EXISTS (SELECT * FROM upsert)")
              .bind("number", number)
              .bind("verification_code", verificationCode)
              .bind("timestamp", timestamp)
              .execute();
      }
    }));
  }

  public Optional<StoredVerificationCode> getCodeForNumber(String number) {
    return database.with(jdbi -> jdbi.withHandle(handle -> {
      try (Timer.Context timer = getCodeForNumberTimer.time()) {
        return handle.createQuery("SELECT verification_code, timestamp, NULL as push_code FROM pending_devices WHERE number = :number")
                     .bind("number", number)
                     .mapTo(StoredVerificationCode.class)
                     .findFirst();
      }
    }));
  }

  public void remove(String number) {
    database.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context timer = removeTimer.time()) {
        handle.createUpdate("DELETE FROM pending_devices WHERE number = :number")
              .bind("number", number)
              .execute();
      }
    }));
  }

}
