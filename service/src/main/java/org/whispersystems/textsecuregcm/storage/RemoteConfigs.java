/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.whispersystems.textsecuregcm.storage.mappers.RemoteConfigRowMapper;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.List;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

public class RemoteConfigs {

  public static final String ID            = "id";
  public static final String NAME          = "name";
  public static final String PERCENTAGE    = "percentage";
  public static final String UUIDS         = "uuids";
  public static final String DEFAULT_VALUE = "default_value";
  public static final String VALUE         = "value";
  public static final String HASH_KEY      = "hash_key";

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Timer          setTimer       = metricRegistry.timer(name(Accounts.class, "set"   ));
  private final Timer          getAllTimer    = metricRegistry.timer(name(Accounts.class, "getAll"));
  private final Timer          deleteTimer    = metricRegistry.timer(name(Accounts.class, "delete"));

  private final FaultTolerantDatabase database;

  public RemoteConfigs(FaultTolerantDatabase database) {
    this.database = database;
    this.database.getDatabase().registerRowMapper(new RemoteConfigRowMapper());
    this.database.getDatabase().registerArrayType(UUID.class, "uuid");
  }

  public void set(RemoteConfig remoteConfig) {
    database.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context ignored = setTimer.time()) {
        handle.createUpdate("INSERT INTO remote_config (" + NAME + ", " + PERCENTAGE + ", " + UUIDS + ", " + DEFAULT_VALUE + ", " + VALUE + ", " + HASH_KEY + ") VALUES (:name, :percentage, :uuids, :default_value, :value, :hash_key) ON CONFLICT(" + NAME + ") DO UPDATE SET " + PERCENTAGE + " = EXCLUDED." + PERCENTAGE + ", " + UUIDS + " = EXCLUDED." + UUIDS + ", " + DEFAULT_VALUE + " = EXCLUDED." + DEFAULT_VALUE + ", " + VALUE + " = EXCLUDED." + VALUE + ", " + HASH_KEY + " = EXCLUDED." + HASH_KEY)
            .bind("name", remoteConfig.getName())
            .bind("percentage", remoteConfig.getPercentage())
            .bind("uuids", remoteConfig.getUuids().toArray(new UUID[0]))
            .bind("default_value", remoteConfig.getDefaultValue())
            .bind("value", remoteConfig.getValue())
            .bind("hash_key", remoteConfig.getHashKey())
            .execute();
      }
    }));
  }

  public List<RemoteConfig> getAll() {
    return database.with(jdbi -> jdbi.withHandle(handle -> {
      try (Timer.Context ignored = getAllTimer.time()) {
        return handle.createQuery("SELECT * FROM remote_config")
                     .mapTo(RemoteConfig.class)
                     .list();
      }
    }));
  }

  public void delete(String name) {
    database.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context ignored = deleteTimer.time()) {
        handle.createUpdate("DELETE FROM remote_config WHERE " + NAME + " = :name")
              .bind("name", name)
              .execute();
      }
    }));
  }
}
