/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import io.github.resilience4j.retry.Retry;
import org.jdbi.v3.core.JdbiException;
import org.jdbi.v3.core.statement.PreparedBatch;
import org.jdbi.v3.core.statement.UnableToExecuteStatementException;
import org.jdbi.v3.core.transaction.SerializableTransactionRunner;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.whispersystems.textsecuregcm.configuration.RetryConfiguration;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.storage.mappers.KeyRecordRowMapper;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static com.codahale.metrics.MetricRegistry.name;

public class Keys {

  private final MetricRegistry metricRegistry  = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter          fallbackMeter   = metricRegistry.meter(name(Keys.class, "fallback"));
  private final Timer          storeTimer      = metricRegistry.timer(name(Keys.class, "store"    ));
  private final Timer          getDevicetTimer = metricRegistry.timer(name(Keys.class, "getDevice"));
  private final Timer          getTimer        = metricRegistry.timer(name(Keys.class, "get"      ));
  private final Timer          getCountTimer   = metricRegistry.timer(name(Keys.class, "getCount" ));
  private final Timer          vacuumTimer     = metricRegistry.timer(name(Keys.class, "vacuum"   ));

  private final FaultTolerantDatabase database;
  private final Retry                 retry;

  public Keys(FaultTolerantDatabase database, RetryConfiguration retryConfiguration) {
    this.database = database;
    this.database.getDatabase().registerRowMapper(new KeyRecordRowMapper());
    this.database.getDatabase().setTransactionHandler(new SerializableTransactionRunner());
    this.database.getDatabase().getConfig(SerializableTransactionRunner.Configuration.class).setMaxRetries(10);

    this.retry = Retry.of("keys", retryConfiguration.toRetryConfigBuilder().build());
  }

  public void store(String number, long deviceId, List<PreKey> keys) {
    retry.executeRunnable(() -> {
      database.use(jdbi -> jdbi.useTransaction(TransactionIsolationLevel.SERIALIZABLE, handle -> {
        try (Timer.Context ignored = storeTimer.time()) {
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
      }));
    });
  }

  public List<KeyRecord> get(String number, long deviceId) {
    try {
      return database.with(jdbi -> jdbi.inTransaction(TransactionIsolationLevel.SERIALIZABLE, handle -> {
        try (Timer.Context ignored = getDevicetTimer.time()) {
          return handle.createQuery("DELETE FROM keys WHERE id IN (SELECT id FROM keys WHERE number = :number AND device_id = :device_id ORDER BY key_id ASC LIMIT 1) RETURNING *")
                       .bind("number", number)
                       .bind("device_id", deviceId)
                       .mapTo(KeyRecord.class)
                       .list();
        }
      }));
    } catch (JdbiException e) {
      // TODO 2021-01-13 Replace this with a retry once desktop clients better handle HTTP/500 responses
      fallbackMeter.mark();
      return Collections.emptyList();
    }
  }

  public List<KeyRecord> get(String number) {
    try {
      return database.with(jdbi -> jdbi.inTransaction(TransactionIsolationLevel.SERIALIZABLE, handle -> {
        try (Timer.Context ignored = getTimer.time()) {
          return handle.createQuery("DELETE FROM keys WHERE id IN (SELECT DISTINCT ON (number, device_id) id FROM keys WHERE number = :number ORDER BY number, device_id, key_id ASC) RETURNING *")
                       .bind("number", number)
                       .mapTo(KeyRecord.class)
                       .list();
        }
      }));
    } catch (JdbiException e) {
      // TODO 2021-01-13 Replace this with a retry once desktop clients better handle HTTP/500 responses
      fallbackMeter.mark();
      return Collections.emptyList();
    }
  }

  public int getCount(String number, long deviceId) {
    return database.with(jdbi -> jdbi.withHandle(handle -> {
      try (Timer.Context ignored = getCountTimer.time()) {
        return handle.createQuery("SELECT COUNT(*) FROM keys WHERE number = :number AND device_id = :device_id")
                     .bind("number", number)
                     .bind("device_id", deviceId)
                     .mapTo(Integer.class)
                     .findOnly();
      }
    }));
  }

  public void delete(final String number) {
    database.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context ignored = getCountTimer.time()) {
        handle.createUpdate("DELETE FROM keys WHERE number = :number")
                .bind("number", number)
                .execute();
      }
    }));
  }

  public void vacuum() {
    database.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context ignored = vacuumTimer.time()) {
        handle.execute("VACUUM keys");
      }
    }));
  }

}
