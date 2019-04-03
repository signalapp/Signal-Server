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
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.jdbi.v3.core.transaction.SerializableTransactionRunner;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.storage.mappers.KeyRecordRowMapper;
import org.whispersystems.textsecuregcm.util.Constants;

import java.security.Key;
import java.util.List;

import static com.codahale.metrics.MetricRegistry.name;

public class Keys {

  private final MetricRegistry metricRegistry  = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Timer          storeTimer      = metricRegistry.timer(name(Keys.class, "store"    ));
  private final Timer          getDevicetTimer = metricRegistry.timer(name(Keys.class, "getDevice"));
  private final Timer          getTimer        = metricRegistry.timer(name(Keys.class, "get"      ));
  private final Timer          getCountTimer   = metricRegistry.timer(name(Keys.class, "getCount" ));
  private final Timer          vacuumTimer     = metricRegistry.timer(name(Keys.class, "vacuum"   ));

  private final Jdbi database;

  public Keys(Jdbi database) {
    this.database = database;
    this.database.registerRowMapper(new KeyRecordRowMapper());
    this.database.setTransactionHandler(new SerializableTransactionRunner());
    this.database.getConfig(SerializableTransactionRunner.Configuration.class).setMaxRetries(10);
  }

  public void store(String number, long deviceId, List<PreKey> keys) {
    database.useTransaction(TransactionIsolationLevel.SERIALIZABLE, handle -> {
      try (Timer.Context timer = storeTimer.time()) {
        PreparedBatch preparedBatch = handle.prepareBatch("INSERT INTO keys (number, device_id, key_id, public_key) VALUES (:number, :device_id, :key_id, :public_key)");

        for (PreKey key : keys) {
          preparedBatch.bind("number", number)
                       .bind("device_id", deviceId)
                       .bind("key_id", key.getKeyId())
                       .bind("public_key", key.getPublicKey())
                       .add();
        }

        handle.createUpdate("DELETE FROM keys WHERE number = :number AND device_id = :device_id")
              .bind("number", number)
              .bind("device_id", deviceId)
              .execute();

        preparedBatch.execute();
      }
    });
  }

  public List<KeyRecord> get(String number, long deviceId) {
    return database.inTransaction(TransactionIsolationLevel.SERIALIZABLE, handle -> {
      try (Timer.Context timer = getDevicetTimer.time()) {
        return handle.createQuery("DELETE FROM keys WHERE id IN (SELECT id FROM keys WHERE number = :number AND device_id = :device_id ORDER BY key_id ASC LIMIT 1) RETURNING *")
                     .bind("number", number)
                     .bind("device_id", deviceId)
                     .mapTo(KeyRecord.class)
                     .list();
      }
    });
  }

  public List<KeyRecord> get(String number) {
    return database.inTransaction(TransactionIsolationLevel.SERIALIZABLE, handle -> {
      try (Timer.Context timer = getTimer.time()) {
        return handle.createQuery("DELETE FROM keys WHERE id IN (SELECT DISTINCT ON (number, device_id) id FROM keys WHERE number = :number ORDER BY number, device_id, key_id ASC) RETURNING *")
                     .bind("number", number)
                     .mapTo(KeyRecord.class)
                     .list();
      }
    });

  }

  public int getCount(String number, long deviceId) {
    return database.withHandle(handle -> {
      try (Timer.Context timer = getCountTimer.time()) {
        return handle.createQuery("SELECT COUNT(*) FROM keys WHERE number = :number AND device_id = :device_id")
                     .bind("number", number)
                     .bind("device_id", deviceId)
                     .mapTo(Integer.class)
                     .findOnly();
      }
    });
  }

  public void vacuum() {
    database.useHandle(handle -> {
      try (Timer.Context timer = vacuumTimer.time()) {
        handle.execute("VACUUM keys");
      }
    });
  }

}
