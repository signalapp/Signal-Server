/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.TestClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;

class ProfileAvatarsTest {

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(DynamoDbExtensionSchema.Tables.PROFILE_AVATARS);

  private ProfileAvatars profileAvatars;

  private static final Duration EXPIRATION = Duration.ofDays(1);
  private static final TestClock CLOCK = TestClock.pinned(Instant.now().truncatedTo(ChronoUnit.SECONDS));

  @BeforeEach
  void setup() {
    profileAvatars = new ProfileAvatars(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DynamoDbExtensionSchema.Tables.PROFILE_AVATARS.tableName(), EXPIRATION, CLOCK);
  }

  @Test
  void testSet() {
    final byte[] identity = TestRandomUtil.nextBytes(32);
    final String avatarUrl1 = "avatar";

    {
      final Optional<String> previousAvatar = profileAvatars.setAvatarUrl(identity, avatarUrl1);
      assertTrue(previousAvatar.isEmpty());
    }

    {
      final GetItemResponse response = DYNAMO_DB_EXTENSION.getDynamoDbClient()
          .getItem(GetItemRequest.builder()
              .tableName(DynamoDbExtensionSchema.Tables.PROFILE_AVATARS.tableName())
              .key(Map.of(ProfileAvatars.KEY_IDENTITY, AttributeValues.fromByteArray(identity)))
              .build());

      assertTrue(response.hasItem());
      assertArrayEquals(identity, response.item().get(ProfileAvatars.KEY_IDENTITY).b().asByteArray());
      assertEquals(avatarUrl1, response.item().get(ProfileAvatars.ATTR_URL).s());
      assertEquals(CLOCK.instant().plus(EXPIRATION),
          Instant.ofEpochSecond(Long.parseLong(response.item().get(ProfileAvatars.ATTR_TTL).n())));
    }

    final String avatarUrl2 = "avatar2";

    {
      final Optional<String> previousAvatar = profileAvatars.setAvatarUrl(identity, avatarUrl2);
      assertEquals(Optional.of(avatarUrl1), previousAvatar);
    }

    {
      final GetItemResponse response = DYNAMO_DB_EXTENSION.getDynamoDbClient()
          .getItem(GetItemRequest.builder()
              .tableName(DynamoDbExtensionSchema.Tables.PROFILE_AVATARS.tableName())
              .key(Map.of(ProfileAvatars.KEY_IDENTITY, AttributeValues.fromByteArray(identity)))
              .build());

      assertTrue(response.hasItem());
      assertArrayEquals(identity, response.item().get(ProfileAvatars.KEY_IDENTITY).b().asByteArray());
      assertEquals(avatarUrl2, response.item().get(ProfileAvatars.ATTR_URL).s());
    }
  }

  @Test
  void testUpdateTtl() {
    final byte[] identity = TestRandomUtil.nextBytes(32);
    final String avatarUrl = "avatar";

    profileAvatars.setAvatarUrl(identity, avatarUrl);

    {
      final GetItemResponse response = DYNAMO_DB_EXTENSION.getDynamoDbClient()
          .getItem(GetItemRequest.builder()
              .tableName(DynamoDbExtensionSchema.Tables.PROFILE_AVATARS.tableName())
              .key(Map.of(ProfileAvatars.KEY_IDENTITY, AttributeValues.fromByteArray(identity)))
              .build());

      assertTrue(response.hasItem());
      assertArrayEquals(identity, response.item().get(ProfileAvatars.KEY_IDENTITY).b().asByteArray());
      assertEquals(CLOCK.instant().plus(EXPIRATION),
          Instant.ofEpochSecond(Long.parseLong(response.item().get(ProfileAvatars.ATTR_TTL).n())));
    }
    final Instant later = CLOCK.instant().plus(Duration.ofDays(30));
    CLOCK.pin(later);

    profileAvatars.updateAvatarTtl(identity);
    {
      final GetItemResponse response = DYNAMO_DB_EXTENSION.getDynamoDbClient()
          .getItem(GetItemRequest.builder()
              .tableName(DynamoDbExtensionSchema.Tables.PROFILE_AVATARS.tableName())
              .key(Map.of(ProfileAvatars.KEY_IDENTITY, AttributeValues.fromByteArray(identity)))
              .build());

      assertTrue(response.hasItem());
      assertArrayEquals(identity, response.item().get(ProfileAvatars.KEY_IDENTITY).b().asByteArray());
      assertEquals(later.plus(EXPIRATION),
          Instant.ofEpochSecond(Long.parseLong(response.item().get(ProfileAvatars.ATTR_TTL).n())));
    }
  }

  @Test
  void testUpdateTtlNotPresent() {
    final byte[] identity = TestRandomUtil.nextBytes(32);

    assertTrue(profileAvatars.updateAvatarTtl(identity).isEmpty());

    final GetItemResponse response = DYNAMO_DB_EXTENSION.getDynamoDbClient()
        .getItem(GetItemRequest.builder()
            .tableName(DynamoDbExtensionSchema.Tables.PROFILE_AVATARS.tableName())
            .key(Map.of(ProfileAvatars.KEY_IDENTITY, AttributeValues.fromByteArray(identity)))
            .build());

    assertFalse(response.hasItem(), "item should not be created as a side effect");
  }

  @Test
  void testDelete() {
    final byte[] identity = TestRandomUtil.nextBytes(32);

    assertTrue(profileAvatars.deleteAvatarUrl(identity).isEmpty());

    final String avatarUrl = "avatar";

    profileAvatars.setAvatarUrl(identity, avatarUrl);

    final Optional<String> deletedUrl = profileAvatars.deleteAvatarUrl(identity);
    assertTrue(deletedUrl.isPresent());
    assertEquals(avatarUrl, deletedUrl.get());

    assertTrue(profileAvatars.deleteAvatarUrl(identity).isEmpty());
  }

}
