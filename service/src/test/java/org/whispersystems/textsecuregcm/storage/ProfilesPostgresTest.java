/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.opentable.db.postgres.embedded.LiquibasePreparer;
import com.opentable.db.postgres.junit5.EmbeddedPostgresExtension;
import com.opentable.db.postgres.junit5.PreparedDbExtension;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.jdbi.v3.core.Jdbi;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.util.Pair;

public class ProfilesPostgresTest extends ProfilesTest {

  @RegisterExtension
  static PreparedDbExtension ACCOUNTS_POSTGRES_EXTENSION =
      EmbeddedPostgresExtension.preparedDatabase(LiquibasePreparer.forClasspathLocation("accountsdb.xml"));

  private Profiles profiles;

  @BeforeEach
  void setUp() {
    final FaultTolerantDatabase faultTolerantDatabase = new FaultTolerantDatabase("profilesTest",
        Jdbi.create(ACCOUNTS_POSTGRES_EXTENSION.getTestDatabase()),
        new CircuitBreakerConfiguration());

    profiles = new Profiles(faultTolerantDatabase);

    faultTolerantDatabase.use(jdbi -> jdbi.useHandle(handle -> handle.createUpdate("DELETE FROM profiles").execute()));
  }

  @Override
  protected ProfilesStore getProfilesStore() {
    return profiles;
  }

  @Test
  void testForEach() {
    UUID uuid = UUID.randomUUID();
    VersionedProfile profileOne = new VersionedProfile("123", "foo", "avatarLocation", null, null,
        null, "aDigest".getBytes());
    VersionedProfile profileTwo = new VersionedProfile("345", "bar", "baz", null, null, null, "boof".getBytes());

    profiles.set(uuid, profileOne);
    profiles.set(uuid, profileTwo);

    final Set<Pair<UUID, VersionedProfile>> retrievedProfiles = new HashSet<>();

    profiles.forEach((u, profile) -> retrievedProfiles.add(new Pair<>(u, profile)), 1);

    assertEquals(Set.of(new Pair<>(uuid, profileOne), new Pair<>(uuid, profileTwo)), retrievedProfiles);
  }

  @Test
  void testForEachDeletedProfiles() {
    UUID uuid = UUID.randomUUID();
    VersionedProfile profileOne = new VersionedProfile("123", "foo", "avatarLocation", null, null,
        null, "aDigest".getBytes());
    VersionedProfile profileTwo = new VersionedProfile("345", "bar", "baz", null, null, null, "boof".getBytes());

    profiles.set(uuid, profileOne);
    profiles.set(UUID.randomUUID(), profileTwo);
    profiles.deleteAll(uuid);

    final List<Pair<UUID, String>> deletedProfiles = new ArrayList<>();

    profiles.forEachDeletedProfile((u, version) -> deletedProfiles.add(new Pair<>(u, version)), 2);

    assertEquals(List.of(new Pair<>(uuid, profileOne.getVersion())), deletedProfiles);
  }
}
