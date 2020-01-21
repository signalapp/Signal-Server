package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.whispersystems.textsecuregcm.storage.mappers.AccountRowMapper;
import org.whispersystems.textsecuregcm.storage.mappers.RemoteConfigRowMapper;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

public class RemoteConfigs {

  public static final String ID         = "id";
  public static final String NAME       = "name";
  public static final String PERCENTAGE = "percentage";
  public static final String UUIDS      = "uuids";

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
        handle.createUpdate("INSERT INTO remote_config (" + NAME + ", " + PERCENTAGE + ", " + UUIDS + ") VALUES (:name, :percentage, :uuids) ON CONFLICT(" + NAME + ") DO UPDATE SET " + PERCENTAGE + " = EXCLUDED." + PERCENTAGE + ", " + UUIDS + " = EXCLUDED." + UUIDS)
              .bind("name", remoteConfig.getName())
              .bind("percentage", remoteConfig.getPercentage())
              .bind("uuids", remoteConfig.getUuids().toArray(new UUID[0]))
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
