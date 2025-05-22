/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
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
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema.Tables;
import org.whispersystems.textsecuregcm.tests.util.ProfileTestHelper;
import org.whispersystems.textsecuregcm.util.AttributeValues;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

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
    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] validAboutEmoji = TestRandomUtil.nextBytes(60);
    final byte[] validAbout = TestRandomUtil.nextBytes(156);
    final String avatar = "profiles/" + ProfileTestHelper.generateRandomBase64FromByteArray(16);
    final byte[] phoneNumberSharing = TestRandomUtil.nextBytes(29);

    validProfile = new VersionedProfile(version, name, avatar, validAboutEmoji, validAbout, null, phoneNumberSharing, commitment);
  }

  @Test
  void testSetGet() {
    profiles.set(ACI, validProfile);

    Optional<VersionedProfile> retrieved = profiles.get(ACI, validProfile.version());

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().name()).isEqualTo(validProfile.name());
    assertThat(retrieved.get().avatar()).isEqualTo(validProfile.avatar());
    assertThat(retrieved.get().commitment()).isEqualTo(validProfile.commitment());
    assertThat(retrieved.get().about()).isEqualTo(validProfile.about());
    assertThat(retrieved.get().aboutEmoji()).isEqualTo(validProfile.aboutEmoji());
  }

  @Test
  void testSetGetAsync() {
    profiles.setAsync(ACI, validProfile).join();

    Optional<VersionedProfile> retrieved = profiles.getAsync(ACI, validProfile.version()).join();

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().name()).isEqualTo(validProfile.name());
    assertThat(retrieved.get().avatar()).isEqualTo(validProfile.avatar());
    assertThat(retrieved.get().commitment()).isEqualTo(validProfile.commitment());
    assertThat(retrieved.get().about()).isEqualTo(validProfile.about());
    assertThat(retrieved.get().aboutEmoji()).isEqualTo(validProfile.aboutEmoji());
  }

  @Test
  void testDeleteReset() throws InvalidInputException {
    profiles.set(ACI, validProfile);

    profiles.deleteAll(ACI).join();

    final String version = "someVersion";
    final byte[] name = TestRandomUtil.nextBytes(81);
    final String differentAvatar = "profiles/" + ProfileTestHelper.generateRandomBase64FromByteArray(16);
    final byte[] differentEmoji = TestRandomUtil.nextBytes(60);
    final byte[] differentAbout = TestRandomUtil.nextBytes(156);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final byte[] phoneNumberSharing = TestRandomUtil.nextBytes(29);
    final byte[] commitment = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    VersionedProfile updatedProfile = new VersionedProfile(version, name, differentAvatar,
        differentEmoji, differentAbout, paymentAddress, phoneNumberSharing, commitment);

    profiles.set(ACI, updatedProfile);

    Optional<VersionedProfile> retrieved = profiles.get(ACI, version);

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().name()).isEqualTo(updatedProfile.name());
    assertThat(retrieved.get().avatar()).isEqualTo(updatedProfile.avatar());
    assertThat(retrieved.get().commitment()).isEqualTo(updatedProfile.commitment());
    assertThat(retrieved.get().about()).isEqualTo(updatedProfile.about());
    assertThat(retrieved.get().aboutEmoji()).isEqualTo(updatedProfile.aboutEmoji());
  }

  @Test
  void testSetGetNullOptionalFields() throws InvalidInputException {
    final String version = "someVersion";
    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] commitment = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    VersionedProfile profile = new VersionedProfile(version, name, null, null, null, null, null,
        commitment);
    profiles.set(ACI, profile);

    Optional<VersionedProfile> retrieved = profiles.get(ACI, version);

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().name()).isEqualTo(profile.name());
    assertThat(retrieved.get().avatar()).isEqualTo(profile.avatar());
    assertThat(retrieved.get().commitment()).isEqualTo(profile.commitment());
    assertThat(retrieved.get().about()).isEqualTo(profile.about());
    assertThat(retrieved.get().aboutEmoji()).isEqualTo(profile.aboutEmoji());
  }

  @Test
  void testSetReplace() throws InvalidInputException {
    profiles.set(ACI, validProfile);

    Optional<VersionedProfile> retrieved = profiles.get(ACI, validProfile.version());

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().name()).isEqualTo(validProfile.name());
    assertThat(retrieved.get().avatar()).isEqualTo(validProfile.avatar());
    assertThat(retrieved.get().commitment()).isEqualTo(validProfile.commitment());
    assertThat(retrieved.get().about()).isEqualTo(validProfile.about());
    assertThat(retrieved.get().aboutEmoji()).isEqualTo(validProfile.aboutEmoji());
    assertThat(retrieved.get().paymentAddress()).isNull();

    final byte[] differentName = TestRandomUtil.nextBytes(81);
    final byte[] differentEmoji = TestRandomUtil.nextBytes(60);
    final byte[] differentAbout = TestRandomUtil.nextBytes(156);
    final String differentAvatar = "profiles/" + ProfileTestHelper.generateRandomBase64FromByteArray(16);
    final byte[] differentPhoneNumberSharing = TestRandomUtil.nextBytes(29);
    final byte[] differentCommitment = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    VersionedProfile updated = new VersionedProfile(validProfile.version(), differentName, differentAvatar, differentEmoji, differentAbout, null,
        differentPhoneNumberSharing, differentCommitment);
    profiles.set(ACI, updated);

    retrieved = profiles.get(ACI, updated.version());

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().name()).isEqualTo(updated.name());
    assertThat(retrieved.get().about()).isEqualTo(updated.about());
    assertThat(retrieved.get().aboutEmoji()).isEqualTo(updated.aboutEmoji());
    assertThat(retrieved.get().avatar()).isEqualTo(updated.avatar());
    assertThat(retrieved.get().phoneNumberSharing()).isEqualTo(updated.phoneNumberSharing());

    // Commitment should be unchanged after an overwrite
    assertThat(retrieved.get().commitment()).isEqualTo(validProfile.commitment());
  }

  @Test
  void testMultipleVersions() throws InvalidInputException {
    final String versionOne = "versionOne";
    final String versionTwo = "versionTwo";

    final byte[] nameOne = TestRandomUtil.nextBytes(81);
    final byte[] nameTwo = TestRandomUtil.nextBytes(81);

    final String avatarOne = "profiles/" + ProfileTestHelper.generateRandomBase64FromByteArray(16);
    final String avatarTwo = "profiles/" + ProfileTestHelper.generateRandomBase64FromByteArray(16);

    final byte[] aboutEmoji = TestRandomUtil.nextBytes(60);
    final byte[] about = TestRandomUtil.nextBytes(156);

    final byte[] commitmentOne = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();
    final byte[] commitmentTwo = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    VersionedProfile profileOne = new VersionedProfile(versionOne, nameOne, avatarOne, null, null,
        null, null, commitmentOne);
    VersionedProfile profileTwo = new VersionedProfile(versionTwo, nameTwo, avatarTwo, aboutEmoji, about, null, null, commitmentTwo);

    profiles.set(ACI, profileOne);
    profiles.set(ACI, profileTwo);

    Optional<VersionedProfile> retrieved = profiles.get(ACI, versionOne);

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().name()).isEqualTo(profileOne.name());
    assertThat(retrieved.get().avatar()).isEqualTo(profileOne.avatar());
    assertThat(retrieved.get().commitment()).isEqualTo(profileOne.commitment());
    assertThat(retrieved.get().about()).isEqualTo(profileOne.about());
    assertThat(retrieved.get().aboutEmoji()).isEqualTo(profileOne.aboutEmoji());

    retrieved = profiles.get(ACI, versionTwo);

    assertThat(retrieved.isPresent()).isTrue();
    assertThat(retrieved.get().name()).isEqualTo(profileTwo.name());
    assertThat(retrieved.get().avatar()).isEqualTo(profileTwo.avatar());
    assertThat(retrieved.get().commitment()).isEqualTo(profileTwo.commitment());
    assertThat(retrieved.get().about()).isEqualTo(profileTwo.about());
    assertThat(retrieved.get().aboutEmoji()).isEqualTo(profileTwo.aboutEmoji());
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
    final String versionThree = "versionThree";

    final byte[] nameOne = TestRandomUtil.nextBytes(81);
    final byte[] nameTwo = TestRandomUtil.nextBytes(81);

    final byte[] aboutEmoji = TestRandomUtil.nextBytes(60);
    final byte[] about = TestRandomUtil.nextBytes(156);

    final String avatarOne = "profiles/" + ProfileTestHelper.generateRandomBase64FromByteArray(16);
    final String avatarTwo = "profiles/" + ProfileTestHelper.generateRandomBase64FromByteArray(16);

    final byte[] commitmentOne = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();
    final byte[] commitmentTwo = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();
    final byte[] commitmentThree = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    VersionedProfile profileOne = new VersionedProfile(versionOne, nameOne, avatarOne, null, null,
        null, null, commitmentOne);
    VersionedProfile profileTwo = new VersionedProfile(versionTwo, nameTwo, avatarTwo, aboutEmoji, about, null, null, commitmentTwo);
    VersionedProfile profileThree = new VersionedProfile(versionThree, nameTwo, null, aboutEmoji, about, null, null,
        commitmentThree);

    profiles.set(ACI, profileOne);
    profiles.set(ACI, profileTwo);
    profiles.set(ACI, profileThree);

    final List<String> avatars = profiles.deleteAll(ACI).join();

    for (String version : List.of(versionOne, versionTwo, versionThree)) {
      final Optional<VersionedProfile> retrieved = profiles.get(ACI, version);
      assertThat(retrieved.isPresent()).isFalse();
    }

    assertThat(avatars.size()).isEqualTo(2);
    assertThat(avatars.containsAll(List.of(avatarOne, avatarTwo))).isTrue();
  }

  @ParameterizedTest
  @MethodSource
  void buildUpdateExpression(final VersionedProfile profile, final String expectedUpdateExpression) {
    assertEquals(expectedUpdateExpression, Profiles.buildUpdateExpression(profile));
  }

  private static Stream<Arguments> buildUpdateExpression() throws InvalidInputException {
    final String version = "someVersion";
    final byte[] name = TestRandomUtil.nextBytes(81);
    final String avatar = "profiles/" + ProfileTestHelper.generateRandomBase64FromByteArray(16);
    final byte[] emoji = TestRandomUtil.nextBytes(60);
    final byte[] about = TestRandomUtil.nextBytes(156);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final byte[] phoneNumberSharing = TestRandomUtil.nextBytes(29);
    final byte[] commitment = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    return Stream.of(
        Arguments.of(
            new VersionedProfile(version, name, avatar, emoji, about, paymentAddress, phoneNumberSharing, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment), #name = :name, #avatar = :avatar, #about = :about, #aboutEmoji = :aboutEmoji, #paymentAddress = :paymentAddress, #phoneNumberSharing = :phoneNumberSharing"),

        Arguments.of(
            new VersionedProfile(version, name, avatar, emoji, about, paymentAddress, null, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment), #name = :name, #avatar = :avatar, #about = :about, #aboutEmoji = :aboutEmoji, #paymentAddress = :paymentAddress REMOVE #phoneNumberSharing"),

        Arguments.of(
            new VersionedProfile(version, name, avatar, emoji, about, null, null, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment), #name = :name, #avatar = :avatar, #about = :about, #aboutEmoji = :aboutEmoji REMOVE #paymentAddress, #phoneNumberSharing"),

        Arguments.of(
            new VersionedProfile(version, name, avatar, emoji, null, null, null, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment), #name = :name, #avatar = :avatar, #aboutEmoji = :aboutEmoji REMOVE #about, #paymentAddress, #phoneNumberSharing"),

        Arguments.of(
            new VersionedProfile(version, name, avatar, null, null, null, null, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment), #name = :name, #avatar = :avatar REMOVE #about, #aboutEmoji, #paymentAddress, #phoneNumberSharing"),

        Arguments.of(
            new VersionedProfile(version, name, null, null, null, null, null, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment), #name = :name REMOVE #avatar, #about, #aboutEmoji, #paymentAddress, #phoneNumberSharing"),

        Arguments.of(
            new VersionedProfile(version, null, null, null, null, null, null, commitment),
            "SET #commitment = if_not_exists(#commitment, :commitment) REMOVE #name, #avatar, #about, #aboutEmoji, #paymentAddress, #phoneNumberSharing")
    );
  }

  @ParameterizedTest
  @MethodSource
  void buildUpdateExpressionAttributeValues(final VersionedProfile profile, final Map<String, AttributeValue> expectedAttributeValues) {
    assertEquals(expectedAttributeValues, Profiles.buildUpdateExpressionAttributeValues(profile));
  }

  private static Stream<Arguments> buildUpdateExpressionAttributeValues() throws InvalidInputException {
    final String version = "someVersion";
    final byte[] name = TestRandomUtil.nextBytes(81);
    final String avatar = "profiles/" + ProfileTestHelper.generateRandomBase64FromByteArray(16);
    final byte[] emoji = TestRandomUtil.nextBytes(60);
    final byte[] about = TestRandomUtil.nextBytes(156);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final byte[] phoneNumberSharing = TestRandomUtil.nextBytes(29);
    final byte[] commitment = new ProfileKey(TestRandomUtil.nextBytes(32)).getCommitment(new ServiceId.Aci(ACI)).serialize();

    return Stream.of(
        Arguments.of(
            new VersionedProfile(version, name, avatar, emoji, about, paymentAddress, phoneNumberSharing, commitment),
            Map.of(
                ":commitment", AttributeValues.fromByteArray(commitment),
                ":name", AttributeValues.fromByteArray(name),
                ":avatar", AttributeValues.fromString(avatar),
                ":aboutEmoji", AttributeValues.fromByteArray(emoji),
                ":about", AttributeValues.fromByteArray(about),
                ":paymentAddress", AttributeValues.fromByteArray(paymentAddress),
                ":phoneNumberSharing", AttributeValues.fromByteArray(phoneNumberSharing))),

        Arguments.of(
            new VersionedProfile(version, name, avatar, emoji, about, paymentAddress, null, commitment),
            Map.of(
                ":commitment", AttributeValues.fromByteArray(commitment),
                ":name", AttributeValues.fromByteArray(name),
                ":avatar", AttributeValues.fromString(avatar),
                ":aboutEmoji", AttributeValues.fromByteArray(emoji),
                ":about", AttributeValues.fromByteArray(about),
                ":paymentAddress", AttributeValues.fromByteArray(paymentAddress))),

        Arguments.of(
            new VersionedProfile(version, name, avatar, emoji, about, null, null, commitment),
            Map.of(
                ":commitment", AttributeValues.fromByteArray(commitment),
                ":name", AttributeValues.fromByteArray(name),
                ":avatar", AttributeValues.fromString(avatar),
                ":aboutEmoji", AttributeValues.fromByteArray(emoji),
                ":about", AttributeValues.fromByteArray(about))),

        Arguments.of(
            new VersionedProfile(version, name, avatar, emoji, null, null, null, commitment),
            Map.of(
                ":commitment", AttributeValues.fromByteArray(commitment),
                ":name",AttributeValues.fromByteArray(name),
                ":avatar", AttributeValues.fromString(avatar),
                ":aboutEmoji", AttributeValues.fromByteArray(emoji))),

        Arguments.of(
            new VersionedProfile(version, name, avatar, null, null, null, null, commitment),
            Map.of(
                ":commitment", AttributeValues.fromByteArray(commitment),
                ":name", AttributeValues.fromByteArray(name),
                ":avatar", AttributeValues.fromString(avatar))),

        Arguments.of(
            new VersionedProfile(version, name, null, null, null, null, null, commitment),
            Map.of(
                ":commitment", AttributeValues.fromByteArray(commitment),
                ":name", AttributeValues.fromByteArray(name))),

        Arguments.of(
            new VersionedProfile(version, null, null, null, null, null, null, commitment),
            Map.of(":commitment", AttributeValues.fromByteArray(commitment)))
    );
  }
}
