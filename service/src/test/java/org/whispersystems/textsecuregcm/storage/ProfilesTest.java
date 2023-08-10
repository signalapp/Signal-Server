/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.profiles.ProfileKey;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Base64;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

@Timeout(value = 10, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
public class ProfilesTest {
  private static final UUID ACI = UUID.randomUUID();
  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION = new DynamoDbExtension(Tables.PROFILES);

  private Profiles profiles;
  private VersionedProfile validProfile;

  @BeforeEach
  void setUp() throws InvalidInputException {
    profiles = new Profiles(DYNAMO_DB_EXTENSION.getDynamoDbClient(),
        DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        Tables.PROFILES.tableName());
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(ACI)).serialize();
    final String version = "someVersion";
    final String name = generateRandomBase64FromByteArray(81);
    final String validAboutEmoji = generateRandomBase64FromByteArray(60);
    final String validAbout = generateRandomBase64FromByteArray(156);
    final String avatar = "profiles/" + generateRandomBase64FromByteArray(16);

    validProfile = new VersionedProfile(version, name, avatar, validAboutEmoji, validAbout, null, commitment);
  }

  @Test
  void testSetGet() {
    profiles.set(ACI, validProfile);

    Optional<VersionedProfile> retrieved = profiles.get(ACI, validProfile.getVersion());

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(validProfile.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(validProfile.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(validProfile.getCommitment());
    assertThat(retrieved.get().getAbout()).isEqualTo(validProfile.getAbout());
    assertThat(retrieved.get().getAboutEmoji()).isEqualTo(validProfile.getAboutEmoji());
  }

  @Test
  void testSetGetAsync() {
    profiles.setAsync(ACI, validProfile).join();

    Optional<VersionedProfile> retrieved = profiles.getAsync(ACI, validProfile.getVersion()).join();

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(validProfile.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(validProfile.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(validProfile.getCommitment());
    assertThat(retrieved.get().getAbout()).isEqualTo(validProfile.getAbout());
    assertThat(retrieved.get().getAboutEmoji()).isEqualTo(validProfile.getAboutEmoji());
  }

  @Test
  void testDeleteReset() throws InvalidInputException {
    profiles.set(ACI, validProfile);

    profiles.deleteAll(ACI);

    final String version = "someVersion";
    final String name = generateRandomBase64FromByteArray(81);
    final String differentAvatar = "profiles/" + generateRandomBase64FromByteArray(16);
    final String differentEmoji = generateRandomBase64FromByteArray(60);
    final String differentAbout = generateRandomBase64FromByteArray(156);
    final String paymentAddress = generateRandomBase64FromByteArray(582);
    final byte[] commitment = new ProfileKey(generateRandomByteArray(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    VersionedProfile updatedProfile = new VersionedProfile(version, name, differentAvatar,
        differentEmoji, differentAbout, paymentAddress, commitment);

    profiles.set(ACI, updatedProfile);

    Optional<VersionedProfile> retrieved = profiles.get(ACI, version);

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(updatedProfile.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(updatedProfile.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(updatedProfile.getCommitment());
    assertThat(retrieved.get().getAbout()).isEqualTo(updatedProfile.getAbout());
    assertThat(retrieved.get().getAboutEmoji()).isEqualTo(updatedProfile.getAboutEmoji());
  }

  @Test
  void testSetGetNullOptionalFields() throws InvalidInputException {
    final String version = "someVersion";
    final String name = generateRandomBase64FromByteArray(81);
    final byte[] commitment = new ProfileKey(generateRandomByteArray(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    VersionedProfile profile = new VersionedProfile(version, name, null, null, null, null,
        commitment);
    profiles.set(ACI, profile);

    Optional<VersionedProfile> retrieved = profiles.get(ACI, version);

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(profile.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(profile.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(profile.getCommitment());
    assertThat(retrieved.get().getAbout()).isEqualTo(profile.getAbout());
    assertThat(retrieved.get().getAboutEmoji()).isEqualTo(profile.getAboutEmoji());
  }

  @Test
  void testSetReplace() throws InvalidInputException {
    profiles.set(ACI, validProfile);

    Optional<VersionedProfile> retrieved = profiles.get(ACI, validProfile.getVersion());

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(validProfile.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(validProfile.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(validProfile.getCommitment());
    assertThat(retrieved.get().getAbout()).isEqualTo(validProfile.getAbout());
    assertThat(retrieved.get().getAboutEmoji()).isEqualTo(validProfile.getAboutEmoji());
    assertThat(retrieved.get().getPaymentAddress()).isNull();

    final String differentName = generateRandomBase64FromByteArray(81);
    final String differentEmoji = generateRandomBase64FromByteArray(60);
    final String differentAbout = generateRandomBase64FromByteArray(156);
    final String differentAvatar = "profiles/" + generateRandomBase64FromByteArray(16);
    final byte[] differentCommitment = new ProfileKey(generateRandomByteArray(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    VersionedProfile updated = new VersionedProfile(validProfile.getVersion(), differentName, differentAvatar, differentEmoji, differentAbout, null,
        differentCommitment);
    profiles.set(ACI, updated);

    retrieved = profiles.get(ACI, updated.getVersion());

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(updated.getName());
    assertThat(retrieved.get().getAbout()).isEqualTo(updated.getAbout());
    assertThat(retrieved.get().getAboutEmoji()).isEqualTo(updated.getAboutEmoji());
    assertThat(retrieved.get().getAvatar()).isEqualTo(updated.getAvatar());

    // Commitment should be unchanged after an overwrite
    assertThat(retrieved.get().getCommitment()).isEqualTo(validProfile.getCommitment());
  }

  @Test
  void testMultipleVersions() throws InvalidInputException {
    final String versionOne = "versionOne";
    final String versionTwo = "versionTwo";

    final String nameOne = generateRandomBase64FromByteArray(81);
    final String nameTwo = generateRandomBase64FromByteArray(81);

    final String avatarOne = "profiles/" + generateRandomBase64FromByteArray(16);
    final String avatarTwo = "profiles/" + generateRandomBase64FromByteArray(16);

    final String aboutEmoji = generateRandomBase64FromByteArray(60);
    final String about = generateRandomBase64FromByteArray(156);

    final byte[] commitmentOne = new ProfileKey(generateRandomByteArray(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();
    final byte[] commitmentTwo = new ProfileKey(generateRandomByteArray(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    VersionedProfile profileOne = new VersionedProfile(versionOne, nameOne, avatarOne, null, null,
        null, commitmentOne);
    VersionedProfile profileTwo = new VersionedProfile(versionTwo, nameTwo, avatarTwo, aboutEmoji, about, null, commitmentTwo);

    profiles.set(ACI, profileOne);
    profiles.set(ACI, profileTwo);

    Optional<VersionedProfile> retrieved = profiles.get(ACI, versionOne);

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(profileOne.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(profileOne.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(profileOne.getCommitment());
    assertThat(retrieved.get().getAbout()).isEqualTo(profileOne.getAbout());
    assertThat(retrieved.get().getAboutEmoji()).isEqualTo(profileOne.getAboutEmoji());

    retrieved = profiles.get(ACI, versionTwo);

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().getName()).isEqualTo(profileTwo.getName());
    assertThat(retrieved.get().getAvatar()).isEqualTo(profileTwo.getAvatar());
    assertThat(retrieved.get().getCommitment()).isEqualTo(profileTwo.getCommitment());
    assertThat(retrieved.get().getAbout()).isEqualTo(profileTwo.getAbout());
    assertThat(retrieved.get().getAboutEmoji()).isEqualTo(profileTwo.getAboutEmoji());
  }

  @Test
  void testMissing() {
    profiles.set(ACI, validProfile);
    final String missingVersion = "missingVersion";

    Optional<VersionedProfile> retrieved = profiles.get(ACI, missingVersion);
    assertThat(retrieved.isPresent()).isFalse();
  }


  @Test
  void testDelete() throws InvalidInputException {
    final String versionOne = "versionOne";
    final String versionTwo = "versionTwo";

    final String nameOne = generateRandomBase64FromByteArray(81);
    final String nameTwo = generateRandomBase64FromByteArray(81);

    final String aboutEmoji = generateRandomBase64FromByteArray(60);
    final String about = generateRandomBase64FromByteArray(156);

    final String avatarOne = "profiles/" + generateRandomBase64FromByteArray(16);
    final String avatarTwo = "profiles/" + generateRandomBase64FromByteArray(16);

    final byte[] commitmentOne = new ProfileKey(generateRandomByteArray(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();
    final byte[] commitmentTwo = new ProfileKey(generateRandomByteArray(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    VersionedProfile profileOne = new VersionedProfile(versionOne, nameOne, avatarOne, null, null,
        null, commitmentOne);
    VersionedProfile profileTwo = new VersionedProfile(versionTwo, nameTwo, avatarTwo, aboutEmoji, about, null, commitmentTwo);

    profiles.set(ACI, profileOne);
    profiles.set(ACI, profileTwo);

    profiles.deleteAll(ACI);

    Optional<VersionedProfile> retrieved = profiles.get(ACI, versionOne);

    assertThat(retrieved.isPresent()).isFalse();

    retrieved = profiles.get(ACI, versionTwo);

    assertThat(retrieved.isPresent()).isFalse();
  }

  @ParameterizedTest
  @MethodSource
  void buildUpdateExpression(final VersionedProfile profile, final String expectedUpdateExpression) {
    assertEquals(expectedUpdateExpression, Profiles.buildUpdateExpression(profile));
  }

  private static Stream<Arguments> buildUpdateExpression() throws InvalidInputException {
    final String version = "someVersion";
    final String name = generateRandomBase64FromByteArray(81);
    final String avatar = "profiles/" + generateRandomBase64FromByteArray(16);;
    final String emoji = generateRandomBase64FromByteArray(60);
    final String about = generateRandomBase64FromByteArray(156);
    final String paymentAddress = generateRandomBase64FromByteArray(582);
    final byte[] commitment = new ProfileKey(generateRandomByteArray(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    return Stream.of(
        Arguments.of(
            new VersionedProfile(version, name, avatar, emoji, about, paymentAddress, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment), #name = :name, #avatar = :avatar, #about = :about, #aboutEmoji = :aboutEmoji, #paymentAddress = :paymentAddress"),

        Arguments.of(
            new VersionedProfile(version, name, avatar, emoji, about, null, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment), #name = :name, #avatar = :avatar, #about = :about, #aboutEmoji = :aboutEmoji REMOVE #paymentAddress"),

        Arguments.of(
            new VersionedProfile(version, name, avatar, emoji, null, null, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment), #name = :name, #avatar = :avatar, #aboutEmoji = :aboutEmoji REMOVE #about, #paymentAddress"),

        Arguments.of(
            new VersionedProfile(version, name, avatar, null, null, null, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment), #name = :name, #avatar = :avatar REMOVE #about, #aboutEmoji, #paymentAddress"),

        Arguments.of(
            new VersionedProfile(version, name, null, null, null, null, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment), #name = :name REMOVE #avatar, #about, #aboutEmoji, #paymentAddress"),

        Arguments.of(
            new VersionedProfile(version, null, null, null, null, null, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment) REMOVE #name, #avatar, #about, #aboutEmoji, #paymentAddress")
    );
  }

  @ParameterizedTest
  @MethodSource
  void buildUpdateExpressionAttributeValues(final VersionedProfile profile, final Map<String, AttributeValue> expectedAttributeValues) {
    assertEquals(expectedAttributeValues, Profiles.buildUpdateExpressionAttributeValues(profile));
  }

  private static Stream<Arguments> buildUpdateExpressionAttributeValues() throws InvalidInputException {
    final String version = "someVersion";
    final String name = generateRandomBase64FromByteArray(81);
    final String avatar = "profiles/" + generateRandomBase64FromByteArray(16);;
    final String emoji = generateRandomBase64FromByteArray(60);
    final String about = generateRandomBase64FromByteArray(156);
    final String paymentAddress = generateRandomBase64FromByteArray(582);
    final byte[] commitment = new ProfileKey(generateRandomByteArray(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    return Stream.of(
        Arguments.of(
            new VersionedProfile(version, name, avatar, emoji, about, paymentAddress, commitment),
            Map.of(
                ":commitment", AttributeValues.fromByteArray(commitment),
                ":name", AttributeValues.fromString(name),
                ":avatar", AttributeValues.fromString(avatar),
                ":aboutEmoji", AttributeValues.fromString(emoji),
                ":about", AttributeValues.fromString(about),
                ":paymentAddress", AttributeValues.fromString(paymentAddress))),

        Arguments.of(
            new VersionedProfile(version, name, avatar, emoji, about, null, commitment),
            Map.of(
                ":commitment", AttributeValues.fromByteArray(commitment),
                ":name", AttributeValues.fromString(name),
                ":avatar", AttributeValues.fromString(avatar),
                ":aboutEmoji", AttributeValues.fromString(emoji),
                ":about", AttributeValues.fromString(about))),

        Arguments.of(
            new VersionedProfile(version, name, avatar, emoji, null, null, commitment),
            Map.of(
                ":commitment", AttributeValues.fromByteArray(commitment),
                ":name", AttributeValues.fromString(name),
                ":avatar", AttributeValues.fromString(avatar),
                ":aboutEmoji", AttributeValues.fromString(emoji))),

        Arguments.of(
            new VersionedProfile(version, name, avatar, null, null, null, commitment),
            Map.of(
                ":commitment", AttributeValues.fromByteArray(commitment),
                ":name", AttributeValues.fromString(name),
                ":avatar", AttributeValues.fromString(avatar))),

        Arguments.of(
            new VersionedProfile(version, name, null, null, null, null, commitment),
            Map.of(
                ":commitment", AttributeValues.fromByteArray(commitment),
                ":name", AttributeValues.fromString(name))),

        Arguments.of(
            new VersionedProfile(version, null, null, null, null, null, commitment),
            Map.of(":commitment", AttributeValues.fromByteArray(commitment)))
    );
  }

  private static String generateRandomBase64FromByteArray(final int byteArrayLength) {
    return Base64.getEncoder().encodeToString(generateRandomByteArray(byteArrayLength));
  }

  private static byte[] generateRandomByteArray(final int length) {
    byte[] byteArray = new byte[length];
    new Random().nextBytes(byteArray);
    return byteArray;
  }
}
