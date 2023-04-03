/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

public abstract class ProfilesTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.PROFILES);

  private Profiles profiles;

  @BeforeEach
  void setUp() {
    profiles = new Profiles(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        Tables.PROFILES.tableName());
  }

  @Test
  void testSetGet() {
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
    UUID uuid = UUID.randomUUID();
    VersionedProfile profile = new VersionedProfile("123", "foo", "avatarLocation", null, null,
        null, "aDigest".getBytes());
    profiles.set(uuid, profile);

    Optional<VersionedProfile> retrieved = profiles.get(uuid, "888");
    assertThat(retrieved.isPresent()).isFalse();
  }


  @Test
  void testDelete() {
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

  @ParameterizedTest
  @MethodSource
  void buildUpdateExpression(final VersionedProfile profile, final String expectedUpdateExpression) {
    assertEquals(expectedUpdateExpression, Profiles.buildUpdateExpression(profile));
  }

  private static Stream<Arguments> buildUpdateExpression() {
    final byte[] commitment = "commitment".getBytes(StandardCharsets.UTF_8);

    return Stream.of(
        Arguments.of(
            new VersionedProfile("version", "name", "avatar", "emoji", "about", "paymentAddress", commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment), #name = :name, #avatar = :avatar, #about = :about, #aboutEmoji = :aboutEmoji, #paymentAddress = :paymentAddress"),

        Arguments.of(
            new VersionedProfile("version", "name", "avatar", "emoji", "about", null, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment), #name = :name, #avatar = :avatar, #about = :about, #aboutEmoji = :aboutEmoji REMOVE #paymentAddress"),

        Arguments.of(
            new VersionedProfile("version", "name", "avatar", "emoji", null, null, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment), #name = :name, #avatar = :avatar, #aboutEmoji = :aboutEmoji REMOVE #about, #paymentAddress"),

        Arguments.of(
            new VersionedProfile("version", "name", "avatar", null, null, null, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment), #name = :name, #avatar = :avatar REMOVE #about, #aboutEmoji, #paymentAddress"),

        Arguments.of(
            new VersionedProfile("version", "name", null, null, null, null, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment), #name = :name REMOVE #avatar, #about, #aboutEmoji, #paymentAddress"),

        Arguments.of(
            new VersionedProfile("version", null, null, null, null, null, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment) REMOVE #name, #avatar, #about, #aboutEmoji, #paymentAddress")
    );
  }

  @ParameterizedTest
  @MethodSource
  void buildUpdateExpressionAttributeValues(final VersionedProfile profile, final Map<String, AttributeValue> expectedAttributeValues) {
    assertEquals(expectedAttributeValues, Profiles.buildUpdateExpressionAttributeValues(profile));
  }

  private static Stream<Arguments> buildUpdateExpressionAttributeValues() {
    final byte[] commitment = "commitment".getBytes(StandardCharsets.UTF_8);

    return Stream.of(
        Arguments.of(
            new VersionedProfile("version", "name", "avatar", "emoji", "about", "paymentAddress", commitment),
            Map.of(
                ":commitment", AttributeValues.fromByteArray(commitment),
                ":name", AttributeValues.fromString("name"),
                ":avatar", AttributeValues.fromString("avatar"),
                ":aboutEmoji", AttributeValues.fromString("emoji"),
                ":about", AttributeValues.fromString("about"),
                ":paymentAddress", AttributeValues.fromString("paymentAddress"))),

        Arguments.of(
            new VersionedProfile("version", "name", "avatar", "emoji", "about", null, commitment),
            Map.of(
                ":commitment", AttributeValues.fromByteArray(commitment),
                ":name", AttributeValues.fromString("name"),
                ":avatar", AttributeValues.fromString("avatar"),
                ":aboutEmoji", AttributeValues.fromString("emoji"),
                ":about", AttributeValues.fromString("about"))),

        Arguments.of(
            new VersionedProfile("version", "name", "avatar", "emoji", null, null, commitment),
            Map.of(
                ":commitment", AttributeValues.fromByteArray(commitment),
                ":name", AttributeValues.fromString("name"),
                ":avatar", AttributeValues.fromString("avatar"),
                ":aboutEmoji", AttributeValues.fromString("emoji"))),

        Arguments.of(
            new VersionedProfile("version", "name", "avatar", null, null, null, commitment),
            Map.of(
                ":commitment", AttributeValues.fromByteArray(commitment),
                ":name", AttributeValues.fromString("name"),
                ":avatar", AttributeValues.fromString("avatar"))),

        Arguments.of(
            new VersionedProfile("version", "name", null, null, null, null, commitment),
            Map.of(
                ":commitment", AttributeValues.fromByteArray(commitment),
                ":name", AttributeValues.fromString("name"))),

        Arguments.of(
            new VersionedProfile("version", null, null, null, null, null, commitment),
            Map.of(":commitment", AttributeValues.fromByteArray(commitment)))
    );
  }
}
