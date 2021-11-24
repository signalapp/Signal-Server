/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.google.common.collect.ImmutableList;
import com.opentable.db.postgres.embedded.LiquibasePreparer;
import com.opentable.db.postgres.junit5.EmbeddedPostgresExtension;
import com.opentable.db.postgres.junit5.PreparedDbExtension;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.result.ResultIterator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.util.Pair;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

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
  }

  @Override
  protected ProfilesStore getProfilesStore() {
    return profiles;
  }

  @Test
  void testGetDeletedProfiles() {
    profiles.purgeDeletedProfiles();

    UUID uuid = UUID.randomUUID();
    VersionedProfile profileOne = new VersionedProfile("123", "foo", "avatarLocation", null, null,
        null, "aDigest".getBytes());
    VersionedProfile profileTwo = new VersionedProfile("345", "bar", "baz", null, null, null, "boof".getBytes());

    profiles.set(uuid, profileOne);
    profiles.set(UUID.randomUUID(), profileTwo);

    profiles.deleteAll(uuid);

    try (final ResultIterator<Pair<UUID, String>> resultIterator = profiles.getDeletedProfiles(10)) {
      assertEquals(List.of(new Pair<>(uuid, profileOne.getVersion())), ImmutableList.copyOf(resultIterator));
    }
  }
}
