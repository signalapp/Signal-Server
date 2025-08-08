/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertRateLimitExceeded;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertStatusException;

import com.google.common.net.InetAddresses;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.signal.chat.common.IdentityType;
import org.signal.chat.common.ServiceIdentifier;
import org.signal.chat.profile.CredentialType;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialRequest;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialResponse;
import org.signal.chat.profile.GetUnversionedProfileRequest;
import org.signal.chat.profile.GetUnversionedProfileResponse;
import org.signal.chat.profile.GetVersionedProfileRequest;
import org.signal.chat.profile.GetVersionedProfileResponse;
import org.signal.chat.profile.ProfileGrpc;
import org.signal.chat.profile.SetProfileRequest;
import org.signal.chat.profile.SetProfileRequest.AvatarChange;
import org.signal.chat.profile.SetProfileResponse;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ServiceId;

import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerPublicParams;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.profiles.ClientZkProfileOperations;
import org.signal.libsignal.zkgroup.profiles.ExpiringProfileKeyCredentialResponse;
import org.signal.libsignal.zkgroup.profiles.ProfileKey;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCommitment;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCredentialRequest;
import org.signal.libsignal.zkgroup.profiles.ProfileKeyCredentialRequestContext;
import org.signal.libsignal.zkgroup.profiles.ServerZkProfileOperations;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessChecksum;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.badges.ProfileBadgeConverter;
import org.whispersystems.textsecuregcm.configuration.BadgeConfiguration;
import org.whispersystems.textsecuregcm.configuration.BadgesConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicPaymentsConfiguration;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.entities.BadgeSvg;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.s3.PolicySigner;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.ProfileTestHelper;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class ProfileGrpcServiceTest extends SimpleBaseGrpcTest<ProfileGrpcService, ProfileGrpc.ProfileBlockingStub> {

  private static final String VERSION = "someVersion";

  private static final byte[] VALID_NAME = new byte[81];

  @Mock
  private AccountsManager accountsManager;

  @Mock
  private ProfilesManager profilesManager;

  @Mock
  private DynamicPaymentsConfiguration dynamicPaymentsConfiguration;

  @Mock
  private VersionedProfile profile;

  @Mock
  private Account account;

  @Mock
  private RateLimiter rateLimiter;

  @Mock
  private ProfileBadgeConverter profileBadgeConverter;

  @Mock
  private ServerZkProfileOperations serverZkProfileOperations;

  private Clock clock;

  @Override
  protected ProfileGrpcService createServiceBeforeEachTest() {
    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    final PolicySigner policySigner = new PolicySigner("accessSecret", "us-west-1");
    final PostPolicyGenerator policyGenerator = new PostPolicyGenerator("us-west-1", "profile-bucket", "accessKey");
    final BadgesConfiguration badgesConfiguration = new BadgesConfiguration(
        List.of(new BadgeConfiguration(
            "TEST",
            "other",
            List.of("l", "m", "h", "x", "xx", "xxx"),
            "SVG",
            List.of(
                new BadgeSvg("sl", "sd"),
                new BadgeSvg("ml", "md"),
                new BadgeSvg("ll", "ld")
            )
        )),
        List.of("TEST1"),
        Map.of(1L, "TEST1", 2L, "TEST2", 3L, "TEST3")
    );
    final RateLimiters rateLimiters = mock(RateLimiters.class);
    final String phoneNumber = PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"),
        PhoneNumberUtil.PhoneNumberFormat.E164);

    getMockRequestAttributesInterceptor().setRequestAttributes(new RequestAttributes(InetAddresses.forString("127.0.0.1"),
        "Signal-Android/1.2.3",
        Locale.LanguageRange.parse("en-us")));

    when(rateLimiters.getProfileLimiter()).thenReturn(rateLimiter);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getPaymentsConfiguration()).thenReturn(dynamicPaymentsConfiguration);

    when(account.getUuid()).thenReturn(AUTHENTICATED_ACI);
    when(account.getNumber()).thenReturn(phoneNumber);
    when(account.getBadges()).thenReturn(Collections.emptyList());

    when(profile.paymentAddress()).thenReturn(null);
    when(profile.avatar()).thenReturn("");

    when(accountsManager.getByAccountIdentifier(any())).thenReturn(Optional.of(account));
    when(accountsManager.update(any(), any())).thenReturn(null);

    when(profilesManager.get(any(), any())).thenReturn(Optional.of(profile));

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getPaymentsConfiguration()).thenReturn(dynamicPaymentsConfiguration);
    when(dynamicPaymentsConfiguration.getDisallowedPrefixes()).thenReturn(Collections.emptyList());

    when(profilesManager.deleteAvatar(anyString())).thenReturn(CompletableFuture.completedFuture(null));

    clock = Clock.fixed(Instant.ofEpochSecond(42), ZoneId.of("Etc/UTC"));

    return new ProfileGrpcService(
        clock,
        accountsManager,
        profilesManager,
        dynamicConfigurationManager,
        badgesConfiguration,
        policyGenerator,
        policySigner,
        profileBadgeConverter,
        rateLimiters,
        serverZkProfileOperations
    );
  }

  @Test
  void setProfile() throws InvalidInputException {
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AUTHENTICATED_ACI)).serialize();
    final byte[] validAboutEmoji = new byte[60];
    final byte[] validAbout = new byte[540];
    final byte[] validPaymentAddress = new byte[582];
    final byte[] validPhoneNumberSharing = new byte[29];

    final SetProfileRequest request = SetProfileRequest.newBuilder()
        .setVersion(VERSION)
        .setName(ByteString.copyFrom(VALID_NAME))
        .setAvatarChange(AvatarChange.AVATAR_CHANGE_UNCHANGED)
        .setAboutEmoji(ByteString.copyFrom(validAboutEmoji))
        .setAbout(ByteString.copyFrom(validAbout))
        .setPaymentAddress(ByteString.copyFrom(validPaymentAddress))
        .setPhoneNumberSharing(ByteString.copyFrom(validPhoneNumberSharing))
        .setCommitment(ByteString.copyFrom(commitment))
        .build();

    authenticatedServiceStub().setProfile(request);

    final ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

    verify(profilesManager).set(eq(account.getUuid()), profileArgumentCaptor.capture());

    final VersionedProfile profile = profileArgumentCaptor.getValue();

    assertThat(profile.commitment()).isEqualTo(commitment);
    assertThat(profile.avatar()).isNull();
    assertThat(profile.version()).isEqualTo(VERSION);
    assertThat(profile.name()).isEqualTo(VALID_NAME);
    assertThat(profile.aboutEmoji()).isEqualTo(validAboutEmoji);
    assertThat(profile.about()).isEqualTo(validAbout);
    assertThat(profile.paymentAddress()).isEqualTo(validPaymentAddress);
    assertThat(profile.phoneNumberSharing()).isEqualTo(validPhoneNumberSharing);
  }

  @ParameterizedTest
  @MethodSource
  void setProfileUpload(final AvatarChange avatarChange, final boolean hasPreviousProfile,
      final boolean expectHasS3UploadPath, final boolean expectDeleteS3Object) throws InvalidInputException {
    final String currentAvatar = "profiles/currentAvatar";
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AUTHENTICATED_ACI)).serialize();

    final SetProfileRequest request = SetProfileRequest.newBuilder()
        .setVersion(VERSION)
        .setName(ByteString.copyFrom(VALID_NAME))
        .setAvatarChange(avatarChange)
        .setCommitment(ByteString.copyFrom(commitment))
        .build();

    when(profile.avatar()).thenReturn(currentAvatar);

    when(profilesManager.get(any(), anyString())).thenReturn(hasPreviousProfile ? Optional.of(profile) : Optional.empty());

    final SetProfileResponse response = authenticatedServiceStub().setProfile(request);

    if (expectHasS3UploadPath) {
      assertTrue(response.getAttributes().getPath().startsWith("profiles/"));
    } else {
      assertEquals(response.getAttributes().getPath(), "");
    }

    if (expectDeleteS3Object) {
      verify(profilesManager).deleteAvatar(currentAvatar);
    } else {
      verify(profilesManager, never()).deleteAvatar(anyString());
    }
  }

  private static Stream<Arguments> setProfileUpload() {
    return Stream.of(
        // Upload new avatar, no previous avatar
        Arguments.of(AvatarChange.AVATAR_CHANGE_UPDATE, false, true, false),
        // Upload new avatar, has previous avatar
        Arguments.of(AvatarChange.AVATAR_CHANGE_UPDATE, true, true, true),
        // Clear avatar on profile, no previous avatar
        Arguments.of(AvatarChange.AVATAR_CHANGE_CLEAR, false, false, false),
        // Clear avatar on profile, has previous avatar
        Arguments.of(AvatarChange.AVATAR_CHANGE_CLEAR, true, false, true),
        // Set same avatar, no previous avatar
        Arguments.of(AvatarChange.AVATAR_CHANGE_UNCHANGED, false, false, false),
        // Set same avatar, has previous avatar
        Arguments.of(AvatarChange.AVATAR_CHANGE_UNCHANGED, true, false, false)
    );
  }

  @ParameterizedTest
  @MethodSource
  void setProfileInvalidRequestData(final SetProfileRequest request) {
    assertStatusException(Status.INVALID_ARGUMENT, () -> authenticatedServiceStub().setProfile(request));
  }

  private static Stream<Arguments> setProfileInvalidRequestData() throws InvalidInputException{
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID_TWO)).serialize();
    final byte[] invalidValue = new byte[42];

    final SetProfileRequest prototypeRequest = SetProfileRequest.newBuilder()
        .setVersion(VERSION)
        .setName(ByteString.copyFrom(VALID_NAME))
        .setCommitment(ByteString.copyFrom(commitment))
        .build();

    return Stream.of(
        // Missing version
        Arguments.of(SetProfileRequest.newBuilder(prototypeRequest)
            .clearVersion()
            .build()),
        // Missing name
        Arguments.of(SetProfileRequest.newBuilder(prototypeRequest)
            .clearName()
            .build()),
        // Invalid name length
        Arguments.of(SetProfileRequest.newBuilder(prototypeRequest)
            .setName(ByteString.copyFrom(invalidValue))
            .build()),
        // Invalid about emoji length
        Arguments.of(SetProfileRequest.newBuilder(prototypeRequest)
            .setAboutEmoji(ByteString.copyFrom(invalidValue))
            .build()),
        // Invalid about length
        Arguments.of(SetProfileRequest.newBuilder(prototypeRequest)
            .setAbout(ByteString.copyFrom(invalidValue))
            .build()),
        // Invalid payment address
        Arguments.of(SetProfileRequest.newBuilder(prototypeRequest)
            .setPaymentAddress(ByteString.copyFrom(invalidValue))
            .build()),
        // Missing profile commitment
        Arguments.of(SetProfileRequest.newBuilder()
            .clearCommitment()
            .build())
    );
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void setPaymentAddressDisallowedCountry(final boolean hasExistingPaymentAddress) throws InvalidInputException {
    final Phonenumber.PhoneNumber disallowedPhoneNumber = PhoneNumberUtil.getInstance().getExampleNumber("CU");
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AUTHENTICATED_ACI)).serialize();

    final byte[] validPaymentAddress = new byte[582];
    if (hasExistingPaymentAddress) {
      when(profile.paymentAddress()).thenReturn(validPaymentAddress);
    }

    final SetProfileRequest request = SetProfileRequest.newBuilder()
        .setVersion(VERSION)
        .setName(ByteString.copyFrom(VALID_NAME))
        .setAvatarChange(AvatarChange.AVATAR_CHANGE_UNCHANGED)
        .setPaymentAddress(ByteString.copyFrom(validPaymentAddress))
        .setCommitment(ByteString.copyFrom(commitment))
        .build();
    final String disallowedCountryCode = String.format("+%d", disallowedPhoneNumber.getCountryCode());
    when(dynamicPaymentsConfiguration.getDisallowedPrefixes()).thenReturn(List.of(disallowedCountryCode));
    when(account.getNumber()).thenReturn(PhoneNumberUtil.getInstance().format(
        disallowedPhoneNumber,
        PhoneNumberUtil.PhoneNumberFormat.E164));
    when(profilesManager.get(any(), anyString())).thenReturn(Optional.of(profile));

    if (hasExistingPaymentAddress) {
      assertDoesNotThrow(() -> authenticatedServiceStub().setProfile(request),
          "Payment address changes in disallowed countries should still be allowed if the account already has a valid payment address");
    } else {
      assertStatusException(Status.PERMISSION_DENIED, () -> authenticatedServiceStub().setProfile(request));
    }
  }

  @Test
  void setProfileBadges() throws InvalidInputException {

    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AUTHENTICATED_ACI)).serialize();

    final SetProfileRequest request = SetProfileRequest.newBuilder()
        .setVersion(VERSION)
        .setName(ByteString.copyFrom(VALID_NAME))
        .setAvatarChange(AvatarChange.AVATAR_CHANGE_UNCHANGED)
        .addAllBadgeIds(List.of("TEST3"))
        .setCommitment(ByteString.copyFrom(commitment))
        .build();

    final int accountsManagerUpdateRetryCount = 2;
    AccountsHelper.setupMockUpdateWithRetries(accountsManager, accountsManagerUpdateRetryCount);
    // set up two invocations -- one for each AccountsManager#update try
    when(account.getBadges())
        .thenReturn(List.of(new AccountBadge("TEST3", Instant.ofEpochSecond(41), false)))
        .thenReturn(List.of(new AccountBadge("TEST2", Instant.ofEpochSecond(41), true),
            new AccountBadge("TEST3", Instant.ofEpochSecond(41), false)));

    //noinspection ResultOfMethodCallIgnored
    authenticatedServiceStub().setProfile(request);

    //noinspection unchecked
    final ArgumentCaptor<List<AccountBadge>> badgeCaptor = ArgumentCaptor.forClass(List.class);
    verify(account, times(2)).setBadges(refEq(clock), badgeCaptor.capture());
    // since the stubbing of getBadges() is brittle, we need to verify the number of invocations, to protect against upstream changes
    verify(account, times(accountsManagerUpdateRetryCount)).getBadges();

    assertEquals(List.of(
        new AccountBadge("TEST3", Instant.ofEpochSecond(41), true),
        new AccountBadge("TEST2", Instant.ofEpochSecond(41), false)),
        badgeCaptor.getValue());
  }

  @ParameterizedTest
  @EnumSource(value = org.signal.chat.common.IdentityType.class, names = {"IDENTITY_TYPE_ACI", "IDENTITY_TYPE_PNI"})
  void getUnversionedProfile(final IdentityType identityType) throws Exception {
    final UUID targetUuid = UUID.randomUUID();
    final org.whispersystems.textsecuregcm.identity.ServiceIdentifier targetIdentifier =
        identityType == IdentityType.IDENTITY_TYPE_ACI ? new AciServiceIdentifier(targetUuid) : new PniServiceIdentifier(targetUuid);

    final GetUnversionedProfileRequest request = GetUnversionedProfileRequest.newBuilder()
        .setServiceIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(identityType)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(targetUuid)))
            .build())
        .build();
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);
    final ECKeyPair identityKeyPair = ECKeyPair.generate();
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());

    final List<Badge> badges = List.of(new Badge(
        "TEST",
        "other",
        "Test Badge",
        "This badge is in unit tests.",
        List.of("l", "m", "h", "x", "xx", "xxx"),
        "SVG",
        List.of(
            new BadgeSvg("sl", "sd"),
            new BadgeSvg("ml", "md"),
            new BadgeSvg("ll", "ld")))
    );

    when(account.getIdentityKey(IdentityTypeUtil.fromGrpcIdentityType(identityType))).thenReturn(identityKey);
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(true);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(account.getBadges()).thenReturn(Collections.emptyList());
    when(account.hasCapability(any())).thenReturn(false);
    when(profileBadgeConverter.convert(any(), any(), anyBoolean())).thenReturn(badges);
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.of(account));

    final GetUnversionedProfileResponse response = authenticatedServiceStub().getUnversionedProfile(request);

    final byte[] unidentifiedAccessChecksum = UnidentifiedAccessChecksum.generateFor(unidentifiedAccessKey);
    final GetUnversionedProfileResponse prototypeExpectedResponse = GetUnversionedProfileResponse.newBuilder()
        .setIdentityKey(ByteString.copyFrom(identityKey.serialize()))
        .setUnidentifiedAccess(ByteString.copyFrom(unidentifiedAccessChecksum))
        .setUnrestrictedUnidentifiedAccess(true)
        .addAllBadges(ProfileGrpcHelper.buildBadges(badges))
        .build();

    final GetUnversionedProfileResponse expectedResponse;
    if (identityType == IdentityType.IDENTITY_TYPE_PNI) {
      expectedResponse = GetUnversionedProfileResponse.newBuilder(prototypeExpectedResponse)
          .clearUnidentifiedAccess()
          .clearBadges()
          .setUnrestrictedUnidentifiedAccess(false)
          .build();
    } else {
      expectedResponse = prototypeExpectedResponse;
    }

    verify(rateLimiter).validate(AUTHENTICATED_ACI);
    verify(accountsManager).getByServiceIdentifier(targetIdentifier);

    assertEquals(expectedResponse, response);
  }

  @Test
  void getUnversionedProfileTargetAccountNotFound() {
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.empty());

    final GetUnversionedProfileRequest request = GetUnversionedProfileRequest.newBuilder()
        .setServiceIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
            .build())
        .build();

    assertStatusException(Status.NOT_FOUND, () -> authenticatedServiceStub().getUnversionedProfile(request));
  }

  @ParameterizedTest
  @EnumSource(value = org.signal.chat.common.IdentityType.class, names = {"IDENTITY_TYPE_ACI", "IDENTITY_TYPE_PNI"})
  void getUnversionedProfileRatelimited(final IdentityType identityType) throws Exception {
    final Duration retryAfterDuration = Duration.ofMinutes(7);
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.of(account));
    doThrow(new RateLimitExceededException(retryAfterDuration))
        .when(rateLimiter).validate(any(UUID.class));

    final GetUnversionedProfileRequest request = GetUnversionedProfileRequest.newBuilder()
        .setServiceIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(identityType)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
            .build())
        .build();

    assertRateLimitExceeded(retryAfterDuration, () -> authenticatedServiceStub().getUnversionedProfile(request), accountsManager);
  }

  @ParameterizedTest
  @MethodSource
  void getVersionedProfile(final String requestVersion, @Nullable final String accountVersion, final boolean expectResponseHasPaymentAddress) {
    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] emoji = TestRandomUtil.nextBytes(60);
    final byte[] about = TestRandomUtil.nextBytes(156);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final byte[] phoneNumberSharing = TestRandomUtil.nextBytes(29);
    final String avatar = "profiles/" + ProfileTestHelper.generateRandomBase64FromByteArray(16);

    final VersionedProfile profile = new VersionedProfile(accountVersion, name, avatar, emoji, about, paymentAddress,
        phoneNumberSharing, new byte[0]);

    final GetVersionedProfileRequest request = GetVersionedProfileRequest.newBuilder()
        .setAccountIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
            .build())
        .setVersion(requestVersion)
        .build();

    when(account.getCurrentProfileVersion()).thenReturn(Optional.ofNullable(accountVersion));
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.of(account));
    when(profilesManager.get(any(), any())).thenReturn(Optional.of(profile));

    final GetVersionedProfileResponse response = authenticatedServiceStub().getVersionedProfile(request);

    final GetVersionedProfileResponse.Builder expectedResponseBuilder = GetVersionedProfileResponse.newBuilder()
        .setName(ByteString.copyFrom(name))
        .setAbout(ByteString.copyFrom(about))
        .setAboutEmoji(ByteString.copyFrom(emoji))
        .setAvatar(avatar)
        .setPhoneNumberSharing(ByteString.copyFrom(phoneNumberSharing));

    if (expectResponseHasPaymentAddress) {
      expectedResponseBuilder.setPaymentAddress(ByteString.copyFrom(paymentAddress));
    }

    assertEquals(expectedResponseBuilder.build(), response);
  }
  private static Stream<Arguments> getVersionedProfile() {
    return Stream.of(
        Arguments.of("version1", "version1", true),
        Arguments.of("version1", null, true),
        Arguments.of("version1", "version2", false)
    );
  }

  @ParameterizedTest
  @MethodSource
  void getVersionedProfileAccountOrProfileNotFound(final boolean missingAccount, final boolean missingProfile) {
    final GetVersionedProfileRequest request = GetVersionedProfileRequest.newBuilder()
        .setAccountIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
            .build())
        .setVersion("versionWithNoProfile")
        .build();
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(missingAccount ? Optional.empty() : Optional.of(account));
    when(profilesManager.get(any(), any())).thenReturn(missingProfile ? Optional.empty() : Optional.of(profile));

    assertStatusException(Status.NOT_FOUND, () -> authenticatedServiceStub().getVersionedProfile(request));
  }

  private static Stream<Arguments> getVersionedProfileAccountOrProfileNotFound() {
    return Stream.of(
        Arguments.of(true, false),
        Arguments.of(false, true)
    );
  }

  @Test
  void getVersionedProfileRatelimited() {
    final Duration retryAfterDuration = MockUtils.updateRateLimiterResponseToFail(rateLimiter, AUTHENTICATED_ACI, Duration.ofMinutes(7));

    final GetVersionedProfileRequest request = GetVersionedProfileRequest.newBuilder()
        .setAccountIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
            .build())
        .setVersion("someVersion")
        .build();

    assertRateLimitExceeded(retryAfterDuration, () -> authenticatedServiceStub().getVersionedProfile(request), accountsManager, profilesManager);
  }

  @Test
  void getVersionedProfilePniInvalidArgument() {
    final GetVersionedProfileRequest request = GetVersionedProfileRequest.newBuilder()
        .setAccountIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(IdentityType.IDENTITY_TYPE_PNI)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
            .build())
        .setVersion("someVersion")
        .build();

    assertStatusException(Status.INVALID_ARGUMENT, () -> authenticatedServiceStub().getVersionedProfile(request));
  }

  @Test
  void getExpiringProfileKeyCredential() throws InvalidInputException, VerificationFailedException {
    final UUID targetUuid = UUID.randomUUID();

    final ServerSecretParams serverSecretParams = ServerSecretParams.generate();
    final ServerPublicParams serverPublicParams = serverSecretParams.getPublicParams();

    final ServerZkProfileOperations serverZkProfile = new ServerZkProfileOperations(serverSecretParams);
    final ClientZkProfileOperations clientZkProfile = new ClientZkProfileOperations(serverPublicParams);

    final byte[] profileKeyBytes = TestRandomUtil.nextBytes(32);
    final ProfileKey profileKey = new ProfileKey(profileKeyBytes);
    final ProfileKeyCommitment profileKeyCommitment = profileKey.getCommitment(new ServiceId.Aci(targetUuid));
    final ProfileKeyCredentialRequestContext profileKeyCredentialRequestContext =
        clientZkProfile.createProfileKeyCredentialRequestContext(new ServiceId.Aci(targetUuid), profileKey);

    when(account.getUuid()).thenReturn(targetUuid);
    when(profile.commitment()).thenReturn(profileKeyCommitment.serialize());
    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(targetUuid))).thenReturn(Optional.of(account));
    when(profilesManager.get(targetUuid, "someVersion")).thenReturn(Optional.of(profile));

    final ProfileKeyCredentialRequest credentialRequest = profileKeyCredentialRequestContext.getRequest();

    final Instant expiration = Instant.now().plus(org.whispersystems.textsecuregcm.util.ProfileHelper.EXPIRING_PROFILE_KEY_CREDENTIAL_EXPIRATION)
        .truncatedTo(ChronoUnit.DAYS);

    final ExpiringProfileKeyCredentialResponse credentialResponse =
        serverZkProfile.issueExpiringProfileKeyCredential(credentialRequest, new ServiceId.Aci(targetUuid), profileKeyCommitment, expiration);

    when(serverZkProfileOperations.issueExpiringProfileKeyCredential(credentialRequest, new ServiceId.Aci(targetUuid), profileKeyCommitment, expiration))
        .thenReturn(credentialResponse);

    final GetExpiringProfileKeyCredentialRequest request = GetExpiringProfileKeyCredentialRequest.newBuilder()
        .setAccountIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(targetUuid)))
            .build())
        .setCredentialRequest(ByteString.copyFrom(credentialRequest.serialize()))
        .setCredentialType(CredentialType.CREDENTIAL_TYPE_EXPIRING_PROFILE_KEY)
        .setVersion("someVersion")
        .build();

    final GetExpiringProfileKeyCredentialResponse response = authenticatedServiceStub().getExpiringProfileKeyCredential(request);

    assertArrayEquals(credentialResponse.serialize(), response.getProfileKeyCredential().toByteArray());

    verify(serverZkProfileOperations).issueExpiringProfileKeyCredential(credentialRequest, new ServiceId.Aci(targetUuid), profileKeyCommitment, expiration);

    final ClientZkProfileOperations clientZkProfileCipher = new ClientZkProfileOperations(serverPublicParams);
    assertThatNoException().isThrownBy(() ->
        clientZkProfileCipher.receiveExpiringProfileKeyCredential(profileKeyCredentialRequestContext, new ExpiringProfileKeyCredentialResponse(response.getProfileKeyCredential().toByteArray())));
  }

  @Test
  void getExpiringProfileKeyCredentialRateLimited() {
    final Duration retryAfterDuration = MockUtils.updateRateLimiterResponseToFail(
        rateLimiter, AUTHENTICATED_ACI, Duration.ofMinutes(5));
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.of(account));

    final GetExpiringProfileKeyCredentialRequest request = GetExpiringProfileKeyCredentialRequest.newBuilder()
        .setAccountIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
            .build())
        .setCredentialRequest(ByteString.copyFrom("credentialRequest".getBytes(StandardCharsets.UTF_8)))
        .setCredentialType(CredentialType.CREDENTIAL_TYPE_EXPIRING_PROFILE_KEY)
        .setVersion("someVersion")
        .build();

    assertRateLimitExceeded(retryAfterDuration, () -> authenticatedServiceStub().getExpiringProfileKeyCredential(request), profilesManager);
  }

  @ParameterizedTest
  @MethodSource
  void getExpiringProfileKeyCredentialAccountOrProfileNotFound(final boolean missingAccount,
      final boolean missingProfile) {
    final UUID targetUuid = UUID.randomUUID();

    when(account.getUuid()).thenReturn(targetUuid);
    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(targetUuid)))
        .thenReturn(missingAccount ? Optional.empty() : Optional.of(account));
    when(profilesManager.get(targetUuid, "someVersion"))
        .thenReturn(missingProfile ? Optional.empty() : Optional.of(profile));

    final GetExpiringProfileKeyCredentialRequest request = GetExpiringProfileKeyCredentialRequest.newBuilder()
        .setAccountIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(targetUuid)))
            .build())
        .setCredentialRequest(ByteString.copyFrom("credentialRequest".getBytes(StandardCharsets.UTF_8)))
        .setCredentialType(CredentialType.CREDENTIAL_TYPE_EXPIRING_PROFILE_KEY)
        .setVersion("someVersion")
        .build();

    assertStatusException(Status.NOT_FOUND, () -> authenticatedServiceStub().getExpiringProfileKeyCredential(request));
  }

  private static Stream<Arguments> getExpiringProfileKeyCredentialAccountOrProfileNotFound() {
    return Stream.of(
        Arguments.of(true, false),
        Arguments.of(false, true)
    );
  }

  @ParameterizedTest
  @MethodSource
  void getExpiringProfileKeyCredentialInvalidArgument(final IdentityType identityType, final CredentialType credentialType,
      final boolean throwZkVerificationException) throws VerificationFailedException {
    final UUID targetUuid = UUID.randomUUID();

    if (throwZkVerificationException) {
      when(serverZkProfileOperations.issueExpiringProfileKeyCredential(any(), any(), any(), any())).thenThrow(new VerificationFailedException());
    }

    when(account.getUuid()).thenReturn(targetUuid);
    when(profile.commitment()).thenReturn("commitment".getBytes(StandardCharsets.UTF_8));
    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(targetUuid))).thenReturn(Optional.of(account));
    when(profilesManager.get(targetUuid, "someVersion")).thenReturn(Optional.of(profile));

    final GetExpiringProfileKeyCredentialRequest request = GetExpiringProfileKeyCredentialRequest.newBuilder()
        .setAccountIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(identityType)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(targetUuid)))
            .build())
        .setCredentialRequest(ByteString.copyFrom("credentialRequest".getBytes(StandardCharsets.UTF_8)))
        .setCredentialType(credentialType)
        .setVersion("someVersion")
        .build();

    assertStatusException(Status.INVALID_ARGUMENT, () -> authenticatedServiceStub().getExpiringProfileKeyCredential(request));
  }

  private static Stream<Arguments> getExpiringProfileKeyCredentialInvalidArgument() {
    return Stream.of(
        // Credential type unspecified
        Arguments.of(IdentityType.IDENTITY_TYPE_ACI, CredentialType.CREDENTIAL_TYPE_UNSPECIFIED, false),
        // Illegal identity type
        Arguments.of(IdentityType.IDENTITY_TYPE_PNI, CredentialType.CREDENTIAL_TYPE_EXPIRING_PROFILE_KEY, false),
        // Artificially fails zero knowledge verification
        Arguments.of(IdentityType.IDENTITY_TYPE_ACI, CredentialType.CREDENTIAL_TYPE_EXPIRING_PROFILE_KEY, true)
    );
  }
}
