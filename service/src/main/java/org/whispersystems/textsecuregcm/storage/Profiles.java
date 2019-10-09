package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.whispersystems.textsecuregcm.storage.mappers.VersionedProfileMapper;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

public class Profiles {

  public static final String ID            = "id";
  public static final String UID           = "uuid";
  public static final String VERSION       = "version";
  public static final String NAME          = "name";
  public static final String AVATAR        = "avatar";
  public static final String COMMITMENT    = "commitment";

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);

  private final Timer setTimer    = metricRegistry.timer(name(Profiles.class, "set"   ));
  private final Timer getTimer    = metricRegistry.timer(name(Profiles.class, "get"   ));
  private final Timer deleteTimer = metricRegistry.timer(name(Profiles.class, "delete"));

  private final FaultTolerantDatabase database;

  public Profiles(FaultTolerantDatabase database) {
    this.database = database;
    this.database.getDatabase().registerRowMapper(new VersionedProfileMapper());
  }

  public void set(UUID uuid, VersionedProfile profile) {
    database.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context ignored = setTimer.time()) {
        handle.createUpdate("INSERT INTO profiles (" + UID + ", " + VERSION + ", " + NAME + ", " + AVATAR + ", " + COMMITMENT + ") VALUES (:uuid, :version, :name, :avatar, :commitment) ON CONFLICT (" + UID + ", " + VERSION + ") DO UPDATE SET " + NAME + " = EXCLUDED." + NAME + ", " + AVATAR + " = EXCLUDED." + AVATAR)
              .bind("uuid", uuid)
              .bind("version", profile.getVersion())
              .bind("name", profile.getName())
              .bind("avatar", profile.getAvatar())
              .bind("commitment", profile.getCommitment())
              .execute();
      }
    }));
  }

  public Optional<VersionedProfile> get(UUID uuid, String version) {
    return database.with(jdbi -> jdbi.withHandle(handle -> {
      try (Timer.Context ignored = getTimer.time()) {
        return handle.createQuery("SELECT * FROM profiles WHERE " + UID + " = :uuid AND " + VERSION + " = :version")
                     .bind("uuid", uuid)
                     .bind("version", version)
                     .mapTo(VersionedProfile.class)
                     .findFirst();
      }
    }));
  }

  public void deleteAll(UUID uuid) {
    database.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context ignored = deleteTimer.time()) {
        handle.createUpdate("DELETE FROM profiles WHERE " + UID + " = :uuid")
              .bind("uuid", uuid)
              .execute();
      }
    }));
  }
}
