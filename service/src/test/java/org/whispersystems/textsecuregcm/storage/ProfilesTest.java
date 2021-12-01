/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.Test;

public abstract class ProfilesTest {

  protected abstract ProfilesStore getProfilesStore();

  @Test
  void testSetGet() {
    ProfilesStore profiles = getProfilesStore();
    UUID uuid = UUID.randomUUID();
    VersionedProfile profile = new VersionedProfile("123", "foo", "avatarLocation", "emoji",
        "the very model of a modern major general",
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
  void testDeleteReset() {
    ProfilesStore profiles = getProfilesStore();
    UUID uuid = UUID.randomUUID();
    profiles.set(uuid, new VersionedProfile("123", "foo", "avatarLocation", "emoji",
        "the very model of a modern major general",
        null, "acommitment".getBytes()));

    profiles.deleteAll(uuid);

    VersionedProfile updatedProfile = new VersionedProfile("123", "name", "differentAvatarLocation",
        "differentEmoji", "changed text", "paymentAddress", "differentcommitment".getBytes(StandardCharsets.UTF_8));

    profiles.set(uuid, updatedProfile);

    Optional<VersionedProfile> retrieved = profiles.get(uuid, "123");

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(updatedProfile.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(updatedProfile.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(updatedProfile.getCommitment());
    assertThat(retrieved.get().getAbout()).isEqualTo(updatedProfile.getAbout());
    assertThat(retrieved.get().getAboutEmoji()).isEqualTo(updatedProfile.getAboutEmoji());
  }

  @Test
  void testSetGetNullOptionalFields() {
    ProfilesStore profiles = getProfilesStore();
    UUID uuid = UUID.randomUUID();
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
  void testSetReplace() {
    ProfilesStore profiles = getProfilesStore();
    UUID uuid = UUID.randomUUID();
    VersionedProfile profile = new VersionedProfile("123", "foo", "avatarLocation", null, null,
        "paymentAddress", "acommitment".getBytes());
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
    assertThat(retrieved.get().getAbout()).isEqualTo(updated.getAbout());
    assertThat(retrieved.get().getAboutEmoji()).isEqualTo(updated.getAboutEmoji());
    assertThat(retrieved.get().getAvatar()).isEqualTo(updated.getAvatar());

    // Commitment should be unchanged after an overwrite
    assertThat(retrieved.get().getCommitment()).isEqualTo(profile.getCommitment());
  }

  @Test
  void testMultipleVersions() {
    ProfilesStore profiles = getProfilesStore();
    UUID uuid = UUID.randomUUID();
    VersionedProfile profileOne = new VersionedProfile("123", "foo", "avatarLocation", null, null,
        null, "acommitmnet".getBytes());
    VersionedProfile profileTwo = new VersionedProfile("345", "bar", "baz", "emoji",
        "i keep typing emoju for some reason",
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
  void testMissing() {
    ProfilesStore profiles = getProfilesStore();
    UUID uuid = UUID.randomUUID();
    VersionedProfile profile = new VersionedProfile("123", "foo", "avatarLocation", null, null,
        null, "aDigest".getBytes());
    profiles.set(uuid, profile);

    Optional<VersionedProfile> retrieved = profiles.get(uuid, "888");
    assertThat(retrieved.isPresent()).isFalse();
  }


  @Test
  void testDelete() {
    ProfilesStore profiles = getProfilesStore();
    UUID uuid = UUID.randomUUID();
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
