/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.storage;

import com.opentable.db.postgres.embedded.LiquibasePreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import org.jdbi.v3.core.Jdbi;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.storage.FaultTolerantDatabase;
import org.whispersystems.textsecuregcm.storage.Profiles;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;

import java.util.Optional;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ProfilesTest {

  @Rule
  public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(LiquibasePreparer.forClasspathLocation("accountsdb.xml"));

  private Profiles profiles;

  @Before
  public void setupProfilesDao() {
    FaultTolerantDatabase faultTolerantDatabase = new FaultTolerantDatabase("profilesTest",
                                                                            Jdbi.create(db.getTestDatabase()),
                                                                            new CircuitBreakerConfiguration());

    this.profiles = new Profiles(faultTolerantDatabase);
  }

  @Test
  public void testSetGet() {
    UUID             uuid    = UUID.randomUUID();
    VersionedProfile profile = new VersionedProfile("123", "foo", "avatarLocation", "emoji", "the very model of a modern major general",
        null, "acommitment".getBytes());
    profiles.set(uuid, profile);

    Optional<VersionedProfile> retrieved = profiles.get(uuid, "123");

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(profile.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(profile.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(profile.getCommitment());
    assertThat(retrieved.get().getAbout()).isEqualTo(profile.getAbout());
    assertThat(retrieved.get().getAboutEmoji()).isEqualTo(profile.getAboutEmoji());
  }

  @Test
  public void testSetGetNullOptionalFields() {
    UUID             uuid    = UUID.randomUUID();
    VersionedProfile profile = new VersionedProfile("123", "foo", null, null, null, null,
        "acommitment".getBytes());
    profiles.set(uuid, profile);

    Optional<VersionedProfile> retrieved = profiles.get(uuid, "123");

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(profile.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(profile.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(profile.getCommitment());
    assertThat(retrieved.get().getAbout()).isEqualTo(profile.getAbout());
    assertThat(retrieved.get().getAboutEmoji()).isEqualTo(profile.getAboutEmoji());
  }

  @Test
  public void testSetReplace() {
    UUID             uuid    = UUID.randomUUID();
    VersionedProfile profile = new VersionedProfile("123", "foo", "avatarLocation", null, null,
        null, "acommitment".getBytes());
    profiles.set(uuid, profile);

    Optional<VersionedProfile> retrieved = profiles.get(uuid, "123");

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(profile.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(profile.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(profile.getCommitment());
    assertThat(retrieved.get().getAbout()).isNull();
    assertThat(retrieved.get().getAboutEmoji()).isNull();

    VersionedProfile updated = new VersionedProfile("123", "bar", "baz", "emoji", "bio", null,
        "boof".getBytes());
    profiles.set(uuid, updated);

    retrieved = profiles.get(uuid, "123");

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(updated.getName());
    assertThat(retrieved.get().getCommitment()).isEqualTo(profile.getCommitment());
    assertThat(retrieved.get().getAbout()).isEqualTo(updated.getAbout());
    assertThat(retrieved.get().getAboutEmoji()).isEqualTo(updated.getAboutEmoji());

    // Commitment should be unchanged after an overwrite
    assertThat(retrieved.get().getAvatar()).isEqualTo(updated.getAvatar());
  }

  @Test
  public void testMultipleVersions() {
    UUID             uuid    = UUID.randomUUID();
    VersionedProfile profileOne = new VersionedProfile("123", "foo", "avatarLocation", null, null,
        null, "acommitmnet".getBytes());
    VersionedProfile profileTwo = new VersionedProfile("345", "bar", "baz", "emoji", "i keep typing emoju for some reason",
        null, "boof".getBytes());

    profiles.set(uuid, profileOne);
    profiles.set(uuid, profileTwo);

    Optional<VersionedProfile> retrieved = profiles.get(uuid, "123");

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(profileOne.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(profileOne.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(profileOne.getCommitment());
    assertThat(retrieved.get().getAbout()).isEqualTo(profileOne.getAbout());
    assertThat(retrieved.get().getAboutEmoji()).isEqualTo(profileOne.getAboutEmoji());

    retrieved = profiles.get(uuid, "345");

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(profileTwo.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(profileTwo.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(profileTwo.getCommitment());
    assertThat(retrieved.get().getAbout()).isEqualTo(profileTwo.getAbout());
    assertThat(retrieved.get().getAboutEmoji()).isEqualTo(profileTwo.getAboutEmoji());
  }

  @Test
  public void testMissing() {
    UUID             uuid    = UUID.randomUUID();
    VersionedProfile profile = new VersionedProfile("123", "foo", "avatarLocation", null, null,
        null, "aDigest".getBytes());
    profiles.set(uuid, profile);

    Optional<VersionedProfile> retrieved = profiles.get(uuid, "888");
    assertThat(retrieved.isPresent()).isFalse();
  }


  @Test
  public void testDelete() {
    UUID             uuid    = UUID.randomUUID();
    VersionedProfile profileOne = new VersionedProfile("123", "foo", "avatarLocation", null, null,
        null, "aDigest".getBytes());
    VersionedProfile profileTwo = new VersionedProfile("345", "bar", "baz", null, null, null, "boof".getBytes());

    profiles.set(uuid, profileOne);
    profiles.set(uuid, profileTwo);

    profiles.deleteAll(uuid);

    Optional<VersionedProfile> retrieved = profiles.get(uuid, "123");

    assertThat(retrieved.isPresent()).isFalse();

    retrieved = profiles.get(uuid, "345");

    assertThat(retrieved.isPresent()).isFalse();
  }


}
