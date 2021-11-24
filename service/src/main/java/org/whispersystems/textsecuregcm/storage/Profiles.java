/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import java.util.Optional;
import java.util.UUID;
import org.whispersystems.textsecuregcm.storage.mappers.VersionedProfileMapper;
import org.whispersystems.textsecuregcm.util.Constants;

public class Profiles implements ProfilesStore {

  public static final String ID = "id";
  public static final String UID = "uuid";
  public static final String VERSION = "version";
  public static final String NAME = "name";
  public static final String AVATAR = "avatar";
  public static final String ABOUT_EMOJI = "about_emoji";
  public static final String ABOUT = "about";
  public static final String PAYMENT_ADDRESS = "payment_address";
  public static final String COMMITMENT = "commitment";
  public static final String DELETED = "deleted";

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);

  private final Timer setTimer    = metricRegistry.timer(name(Profiles.class, "set"   ));
  private final Timer getTimer    = metricRegistry.timer(name(Profiles.class, "get"   ));
  private final Timer deleteTimer = metricRegistry.timer(name(Profiles.class, "delete"));

  private final FaultTolerantDatabase database;

  public Profiles(FaultTolerantDatabase database) {
    this.database = database;
    this.database.getDatabase().registerRowMapper(new VersionedProfileMapper());
  }

  @Override
  public void set(UUID uuid, VersionedProfile profile) {
    database.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context ignored = setTimer.time()) {
        handle.createUpdate(
            "INSERT INTO profiles ("
                + UID + ", "
                + VERSION + ", "
                + NAME + ", "
                + AVATAR + ", "
                + ABOUT_EMOJI + ", "
                + ABOUT + ", "
                + PAYMENT_ADDRESS + ", "
                + COMMITMENT + ") "
                + "VALUES (:uuid, :version, :name, :avatar, :about_emoji, :about, :payment_address, :commitment) "
                + "ON CONFLICT (" + UID + ", " + VERSION + ") "
                + "DO UPDATE SET "
                + NAME + " = EXCLUDED." + NAME + ", "
                + AVATAR + " = EXCLUDED." + AVATAR + ", "
                + ABOUT + " = EXCLUDED." + ABOUT + ", "
                + ABOUT_EMOJI + " = EXCLUDED." + ABOUT_EMOJI + ", "
                + PAYMENT_ADDRESS + " = EXCLUDED." + PAYMENT_ADDRESS + ", "
                + DELETED + " = FALSE, "
                + COMMITMENT + " = CASE WHEN profiles." + DELETED + " = TRUE THEN EXCLUDED." + COMMITMENT + " ELSE profiles." + COMMITMENT + " END")
            .bind("uuid", uuid)
            .bind("version", profile.getVersion())
            .bind("name", profile.getName())
            .bind("avatar", profile.getAvatar())
            .bind("about_emoji", profile.getAboutEmoji())
            .bind("about", profile.getAbout())
            .bind("payment_address", profile.getPaymentAddress())
            .bind("commitment", profile.getCommitment())
            .execute();
      }
    }));
  }

  @Override
  public Optional<VersionedProfile> get(UUID uuid, String version) {
    return database.with(jdbi -> jdbi.withHandle(handle -> {
      try (Timer.Context ignored = getTimer.time()) {
        return handle.createQuery("SELECT * FROM profiles WHERE " + UID + " = :uuid AND " + VERSION + " = :version AND " + DELETED + "= FALSE")
                     .bind("uuid", uuid)
                     .bind("version", version)
                     .mapTo(VersionedProfile.class)
                     .findFirst();
      }
    }));
  }

  @Override
  public void deleteAll(UUID uuid) {
    database.use(jdbi -> jdbi.useHandle(handle -> {
      try (Timer.Context ignored = deleteTimer.time()) {
        handle.createUpdate("UPDATE profiles SET " + DELETED + " = TRUE WHERE " + UID + " = :uuid")
              .bind("uuid", uuid)
              .execute();
      }
    }));
  }
}
