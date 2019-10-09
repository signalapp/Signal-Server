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
    VersionedProfile profile = new VersionedProfile("123", "foo", "avatarLocation", "acommitment".getBytes());
    profiles.set(uuid, profile);

    Optional<VersionedProfile> retrieved = profiles.get(uuid, "123");

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(profile.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(profile.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(profile.getCommitment());
  }

  @Test
  public void testSetReplace() {
    UUID             uuid    = UUID.randomUUID();
    VersionedProfile profile = new VersionedProfile("123", "foo", "avatarLocation", "acommitment".getBytes());
    profiles.set(uuid, profile);

    Optional<VersionedProfile> retrieved = profiles.get(uuid, "123");

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(profile.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(profile.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(profile.getCommitment());

    VersionedProfile updated = new VersionedProfile("123", "bar", "baz", "boof".getBytes());
    profiles.set(uuid, updated);

    retrieved = profiles.get(uuid, "123");

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(updated.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(updated.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(profile.getCommitment());
  }

  @Test
  public void testMultipleVersions() {
    UUID             uuid    = UUID.randomUUID();
    VersionedProfile profileOne = new VersionedProfile("123", "foo", "avatarLocation", "acommitmnet".getBytes());
    VersionedProfile profileTwo = new VersionedProfile("345", "bar", "baz", "boof".getBytes());

    profiles.set(uuid, profileOne);
    profiles.set(uuid, profileTwo);

    Optional<VersionedProfile> retrieved = profiles.get(uuid, "123");

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(profileOne.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(profileOne.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(profileOne.getCommitment());

    retrieved = profiles.get(uuid, "345");

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(profileTwo.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(profileTwo.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(profileTwo.getCommitment());
  }

  @Test
  public void testMissing() {
    UUID             uuid    = UUID.randomUUID();
    VersionedProfile profile = new VersionedProfile("123", "foo", "avatarLocation", "aDigest".getBytes());
    profiles.set(uuid, profile);

    Optional<VersionedProfile> retrieved = profiles.get(uuid, "888");
    assertThat(retrieved.isPresent()).isFalse();
  }


  @Test
  public void testDelete() {
    UUID             uuid    = UUID.randomUUID();
    VersionedProfile profileOne = new VersionedProfile("123", "foo", "avatarLocation", "aDigest".getBytes());
    VersionedProfile profileTwo = new VersionedProfile("345", "bar", "baz", "boof".getBytes());

    profiles.set(uuid, profileOne);
    profiles.set(uuid, profileTwo);

    profiles.deleteAll(uuid);

    Optional<VersionedProfile> retrieved = profiles.get(uuid, "123");

    assertThat(retrieved.isPresent()).isFalse();

    retrieved = profiles.get(uuid, "345");

    assertThat(retrieved.isPresent()).isFalse();
  }


}
