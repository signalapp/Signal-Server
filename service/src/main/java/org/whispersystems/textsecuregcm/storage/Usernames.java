package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.jdbi.v3.core.JdbiException;
import org.whispersystems.textsecuregcm.storage.mappers.AccountRowMapper;
import org.whispersystems.textsecuregcm.util.Constants;

import java.sql.SQLException;
import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

public class Usernames {

  public static final String ID       = "id";
  public static final String UID      = "uuid";
  public static final String USERNAME = "username";

  private final MetricRegistry metricRegistry     = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Timer          createTimer        = metricRegistry.timer(name(Usernames.class, "create"       ));
  private final Timer          deleteTimer        = metricRegistry.timer(name(Usernames.class, "delete"       ));
  private final Timer          getByUsernameTimer = metricRegistry.timer(name(Usernames.class, "getByUsername"));
  private final Timer          getByUuidTimer     = metricRegistry.timer(name(Usernames.class, "getByUuid"    ));

  private final FaultTolerantDatabase database;

  public Usernames(FaultTolerantDatabase database) {
    this.database = database;
    this.database.getDatabase().registerRowMapper(new AccountRowMapper());
  }

  public boolean put(UUID uuid, String username) {
    return database.with(jdbi -> jdbi.withHandle(handle -> {
      try (Timer.Context ignored = createTimer.time()) {
        int modified = handle.createUpdate("INSERT INTO usernames (" + UID + ", " + USERNAME + ") VALUES (:uuid, :username) ON CONFLICT (" + UID + ") DO UPDATE SET " + USERNAME + " = EXCLUDED.username")
                             .bind("uuid", uuid)
                             .bind("username", username)
                             .execute();

        return modified > 0;
      } catch (JdbiException e) {
        if (e.getCause() instanceof SQLException) {
          if (((SQLException)e.getCause()).getSQLState().equals("23505")) {
            return false;
          }
        }

        throw e;
      }
    }));
  }

  public void delete(UUID uuid) {
    database.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context ignored = deleteTimer.time()) {
        handle.createUpdate("DELETE FROM usernames WHERE " + UID + " = :uuid")
              .bind("uuid", uuid)
              .execute();
      }
    }));
  }

  public Optional<UUID> get(String username) {
    return database.with(jdbi -> jdbi.withHandle(handle -> {
      try (Timer.Context ignored = getByUsernameTimer.time()) {
        return handle.createQuery("SELECT " + UID + " FROM usernames WHERE " + USERNAME + " = :username")
                     .bind("username", username)
                     .mapTo(UUID.class)
                     .findFirst();
      }
    }));
  }

  public Optional<String> get(UUID uuid) {
    return database.with(jdbi -> jdbi.withHandle(handle -> {
      try (Timer.Context ignored = getByUuidTimer.time()) {
        return handle.createQuery("SELECT " + USERNAME + " FROM usernames WHERE " + UID + " = :uuid")
                     .bind("uuid", uuid)
                     .mapTo(String.class)
                     .findFirst();
      }
    }));
  }

}
