/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

class ProfilesDynamoDbTest extends ProfilesTest {

  private static final String PROFILES_TABLE_NAME = "profiles_test";

  @RegisterExtension
  static DynamoDbExtension dynamoDbExtension = DynamoDbExtension.builder()
      .tableName(PROFILES_TABLE_NAME)
      .hashKey(ProfilesDynamoDb.KEY_ACCOUNT_UUID)
      .rangeKey(ProfilesDynamoDb.ATTR_VERSION)
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(ProfilesDynamoDb.KEY_ACCOUNT_UUID)
          .attributeType(ScalarAttributeType.B)
          .build())
      .attributeDefinition(AttributeDefinition.builder()
          .attributeName(ProfilesDynamoDb.ATTR_VERSION)
          .attributeType(ScalarAttributeType.S)
          .build())
      .build();

  private ProfilesDynamoDb profiles;

  @BeforeEach
  void setUp() {
    profiles = new ProfilesDynamoDb(dynamoDbExtension.getDynamoDbClient(),
        dynamoDbExtension.getDynamoDbAsyncClient(),
        PROFILES_TABLE_NAME);
  }

  @Override
  protected ProfilesStore getProfilesStore() {
    return profiles;
  }

  @ParameterizedTest
  @MethodSource
  void buildUpdateExpression(final VersionedProfile profile, final String expectedUpdateExpression) {
    assertEquals(expectedUpdateExpression, ProfilesDynamoDb.buildUpdateExpression(profile));
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
    assertEquals(expectedAttributeValues, ProfilesDynamoDb.buildUpdateExpressionAttributeValues(profile));
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

  @ParameterizedTest
  @MethodSource
  void migrate(final VersionedProfile profile) {
    final UUID uuid = UUID.randomUUID();

    assertTrue(assertDoesNotThrow(() -> profiles.migrate(uuid, profile).join()));
    assertFalse(assertDoesNotThrow(() -> profiles.migrate(uuid, profile).join()));

    assertEquals(Optional.of(profile), profiles.get(uuid, profile.getVersion()));
  }

  private static Stream<Arguments> migrate() {
    return Stream.of(
        Arguments.of(new VersionedProfile("version", "name", "avatar", "emoji", "about", "paymentAddress", "commitment".getBytes(StandardCharsets.UTF_8))),
        Arguments.of(new VersionedProfile("version", null, "avatar", "emoji", "about", "paymentAddress", "commitment".getBytes(StandardCharsets.UTF_8))),
        Arguments.of(new VersionedProfile("version", "name", null, "emoji", "about", "paymentAddress", "commitment".getBytes(StandardCharsets.UTF_8))),
        Arguments.of(new VersionedProfile("version", "name", "avatar", null, "about", "paymentAddress", "commitment".getBytes(StandardCharsets.UTF_8))),
        Arguments.of(new VersionedProfile("version", "name", "avatar", "emoji", null, "paymentAddress", "commitment".getBytes(StandardCharsets.UTF_8))),
        Arguments.of(new VersionedProfile("version", "name", "avatar", "emoji", "about", null, "commitment".getBytes(StandardCharsets.UTF_8)))
    );
  }

  @Test
  void delete() {
    final UUID uuid = UUID.randomUUID();
    final VersionedProfile firstProfile =
        new VersionedProfile("version1", "name", "avatar", "emoji", "about", "paymentAddress", "commitment".getBytes(StandardCharsets.UTF_8));

    final VersionedProfile secondProfile =
        new VersionedProfile("version2", "name", "avatar", "emoji", "about", "paymentAddress", "commitment".getBytes(StandardCharsets.UTF_8));

    profiles.set(uuid, firstProfile);
    profiles.set(uuid, secondProfile);

    profiles.delete(uuid, firstProfile.getVersion()).join();

    assertTrue(profiles.get(uuid, firstProfile.getVersion()).isEmpty());
    assertTrue(profiles.get(uuid, secondProfile.getVersion()).isPresent());
  }
}
