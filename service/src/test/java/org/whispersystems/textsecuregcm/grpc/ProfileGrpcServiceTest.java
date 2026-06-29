/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.ArgumentMatchers.refEq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertRateLimitExceeded;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertStatusException;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertStatusInvalidArgument;

import com.google.common.net.InetAddresses;
import com.google.i18n.phonenumbers.PhoneNumberUtil;
import com.google.i18n.phonenumbers.Phonenumber;
import com.google.protobuf.ByteString;
import com.google.rpc.BadRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
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
import org.signal.chat.profile.DataEtag;
import org.signal.chat.profile.GetAvatarCredentialsRequest;
import org.signal.chat.profile.GetAvatarCredentialsResponse;
import org.signal.chat.profile.GetUnversionedProfileRequest;
import org.signal.chat.profile.GetUnversionedProfileResponse;
import org.signal.chat.profile.GetUnversionedProfileResult;
import org.signal.chat.profile.GetVersionedProfileRequest;
import org.signal.chat.profile.GetVersionedProfileResponse;
import org.signal.chat.profile.GetVersionedProfileResult;
import org.signal.chat.profile.GetVersionedProfileV1Response;
import org.signal.chat.profile.ProfileGrpc;
import org.signal.chat.profile.SetProfileRequest;
import org.signal.chat.profile.SetProfileResponse;
import org.signal.chat.profile.SetProfileResult;
import org.signal.chat.profile.SetProfileV1Request;
import org.signal.chat.profile.SetProfileV1Request.AvatarChange;
import org.signal.chat.profile.test.PlaintextProfileData;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ZkCredentialKeyPair;
import org.signal.libsignal.zkgroup.avatars.AvatarUploadCredentialRequest;
import org.signal.libsignal.zkgroup.avatars.AvatarUploadCredentialRequestContext;
import org.signal.libsignal.zkgroup.avatars.AvatarUploadCredentialResponse;
import org.signal.libsignal.zkgroup.profiles.ProfileKey;
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
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountBadge;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.storage.VersionedProfileV1;
import org.whispersystems.textsecuregcm.storage.WriteConflictException;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.ProfileTestHelper;
import org.whispersystems.textsecuregcm.util.MockUtils;
import org.whispersystems.textsecuregcm.util.MutableClock;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class ProfileGrpcServiceTest extends SimpleBaseGrpcTest<ProfileGrpcService, ProfileGrpc.ProfileBlockingStub> {

  private static final byte[] VERSION = TestRandomUtil.nextBytes(32);

  private static final byte[] VALID_DATA = new byte[128];

  private static final byte[] VALID_NAME = new byte[81];

  private static final SetProfileV1Request V1_REQUEST = SetProfileV1Request.newBuilder()
      .setName(ByteString.copyFrom(VALID_NAME))
      .setPhoneNumberSharing(ByteString.copyFrom(TestRandomUtil.nextBytes(29)))
      .build();

  private final GenericServerSecretParams genericServerSecretParams = GenericServerSecretParams.generate();

  @Mock
  private AccountsManager accountsManager;

  @Mock
  private ProfilesManager profilesManager;

  @Mock
  private DynamicPaymentsConfiguration dynamicPaymentsConfiguration;

  @Mock
  private VersionedProfileV1 profile;

  @Mock
  private Account account;

  @Mock
  private RateLimiter rateLimiter;

  @Mock
  private ProfileBadgeConverter profileBadgeConverter;

  private MutableClock clock;

  @Override
  protected ProfileGrpcService createServiceBeforeEachTest() {
    clock = new MutableClock(Clock.fixed(Instant.ofEpochSecond(42), ZoneId.of("Etc/UTC")));

    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    final PostPolicyGenerator policyGenerator = new PostPolicyGenerator("us-west-1", "profile-bucket", "accessKey", "accessSecret");
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
        "en-us"));

    when(rateLimiters.getProfileLimiter()).thenReturn(rateLimiter);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getPaymentsConfiguration()).thenReturn(dynamicPaymentsConfiguration);

    when(account.getUuid()).thenReturn(AUTHENTICATED_ACI);
    when(account.getIdentifier(org.whispersystems.textsecuregcm.identity.IdentityType.ACI)).thenReturn(AUTHENTICATED_ACI);
    when(account.getNumber()).thenReturn(phoneNumber);
    when(account.getBadges()).thenReturn(Collections.emptyList());
    when(account.hasCapability(DeviceCapability.PROFILES_V2)).thenReturn(true);

    when(profile.paymentAddress()).thenReturn(null);
    when(profile.avatar()).thenReturn("");

    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI)).thenReturn(Optional.of(account));
    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(AUTHENTICATED_ACI))).thenReturn(Optional.of(account));

    when(profilesManager.getV1(any(), any())).thenReturn(Optional.of(profile));

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getPaymentsConfiguration()).thenReturn(dynamicPaymentsConfiguration);
    when(dynamicPaymentsConfiguration.getDisallowedPrefixes()).thenReturn(Collections.emptyList());

    when(profilesManager.deleteAvatar(anyString())).thenReturn(CompletableFuture.completedFuture(null));

    return new ProfileGrpcService(
        clock,
        accountsManager,
        profilesManager,
        dynamicConfigurationManager,
        badgesConfiguration,
        policyGenerator,
        genericServerSecretParams,
        profileBadgeConverter,
        rateLimiters
    );
  }

  @Test
  void setProfile() throws Exception {
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AUTHENTICATED_ACI)).serialize();
    final byte[] validAboutEmoji = new byte[60];
    final byte[] validAbout = new byte[540];
    final byte[] validPaymentAddress = new byte[582];
    final byte[] validPhoneNumberSharing = new byte[29];

    final SetProfileRequest request = SetProfileRequest.newBuilder()
        .setVersion(ByteString.copyFrom(VERSION))
        .setData(ByteString.copyFrom(VALID_DATA))
        .setCommitment(ByteString.copyFrom(commitment)) // after v1 -> v2 migration, commitment can be null if expectedDataHash is present
        .setPaymentAddress(ByteString.copyFrom(validPaymentAddress))
        .setV1Request(V1_REQUEST.toBuilder()
            .setAvatarChange(AvatarChange.AVATAR_CHANGE_UNCHANGED)
            .setAboutEmoji(ByteString.copyFrom(validAboutEmoji))
            .setAbout(ByteString.copyFrom(validAbout))
            .setPhoneNumberSharing(ByteString.copyFrom(validPhoneNumberSharing))
        )
        .build();

    authenticatedServiceStub().setProfile(request);

    final ArgumentCaptor<VersionedProfileV1> profileV1ArgumentCaptor = ArgumentCaptor.forClass(VersionedProfileV1.class);
    final ArgumentCaptor<VersionedProfile> profileArgumentCaptor = ArgumentCaptor.forClass(VersionedProfile.class);

    verify(profilesManager).set(eq(account.getUuid()), profileV1ArgumentCaptor.capture(), profileArgumentCaptor.capture(), isNull());

    final VersionedProfile profile = profileArgumentCaptor.getValue();

    assertThat(profile.version()).isEqualTo(VERSION);
    assertThat(profile.data()).isEqualTo(VALID_DATA);
    assertThat(profile.dataHash()).hasSize(32);
    assertThat(profile.commitment()).isEqualTo(commitment);
    assertThat(profile.paymentAddress()).isEqualTo(validPaymentAddress);
    assertThat(profile.paymentAddressHash()).hasSize(32);

    final VersionedProfileV1 v1Profile = profileV1ArgumentCaptor.getValue();

    assertThat(v1Profile.commitment()).isEqualTo(commitment);
    assertThat(v1Profile.avatar()).isNull();
    assertThat(v1Profile.version()).isEqualTo(HexFormat.of().formatHex(VERSION));
    assertThat(v1Profile.name()).isEqualTo(VALID_NAME);
    assertThat(v1Profile.aboutEmoji()).isEqualTo(validAboutEmoji);
    assertThat(v1Profile.about()).isEqualTo(validAbout);
    assertThat(v1Profile.paymentAddress()).isEqualTo(validPaymentAddress);
    assertThat(v1Profile.phoneNumberSharing()).isEqualTo(validPhoneNumberSharing);
  }

  @Test
  void setProfileExpectedDataWriteConflict() throws Exception {
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AUTHENTICATED_ACI)).serialize();
    final byte[] validAboutEmoji = new byte[60];
    final byte[] validAbout = new byte[540];
    final byte[] validPaymentAddress = new byte[582];
    final byte[] validPhoneNumberSharing = new byte[29];

    doThrow(WriteConflictException.class)
        .when(profilesManager).set(any(), any(), any(), any());

    final SetProfileRequest request = SetProfileRequest.newBuilder()
        .setVersion(ByteString.copyFrom(VERSION))
        .setData(ByteString.copyFrom(VALID_DATA))
        .setCommitment(ByteString.copyFrom(commitment)) // after v1 -> v2 migration, commitment can be null if expectedDataHash is present
        .setPaymentAddress(ByteString.copyFrom(validPaymentAddress))
        .setV1Request(V1_REQUEST.toBuilder()
            .setAvatarChange(AvatarChange.AVATAR_CHANGE_UNCHANGED)
            .setAboutEmoji(ByteString.copyFrom(validAboutEmoji))
            .setAbout(ByteString.copyFrom(validAbout))
            .setPhoneNumberSharing(ByteString.copyFrom(validPhoneNumberSharing))
        )
        .build();

    final SetProfileResponse response = authenticatedServiceStub().setProfile(request);

    assertTrue(response.hasExpectedDataWriteConflict());
    verify(accountsManager, never()).updateCurrentProfileVersion(any(), any(), any(), any());
  }

  @Test
  void setProfileExpectedVersionWriteConflictBeforeUpdates() throws Exception {
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AUTHENTICATED_ACI)).serialize();
    final byte[] validAboutEmoji = new byte[60];
    final byte[] validAbout = new byte[540];
    final byte[] validPaymentAddress = new byte[582];
    final byte[] validPhoneNumberSharing = new byte[29];

    when(account.getCurrentProfileVersion()).thenReturn(
        Optional.of(TestRandomUtil.nextBytes(32)));

    final SetProfileRequest request = SetProfileRequest.newBuilder()
        .setVersion(ByteString.copyFrom(VERSION))
        .setData(ByteString.copyFrom(VALID_DATA))
        .setCommitment(ByteString.copyFrom(commitment)) // after v1 -> v2 migration, commitment can be null if expectedDataHash is present
        .setPaymentAddress(ByteString.copyFrom(validPaymentAddress))
        .setExpectedCurrentVersion(ByteString.copyFrom(VERSION))
        .setV1Request(V1_REQUEST.toBuilder()
            .setAvatarChange(AvatarChange.AVATAR_CHANGE_UNCHANGED)
            .setAboutEmoji(ByteString.copyFrom(validAboutEmoji))
            .setAbout(ByteString.copyFrom(validAbout))
            .setPhoneNumberSharing(ByteString.copyFrom(validPhoneNumberSharing))
        )
        .build();

    final SetProfileResponse response = authenticatedServiceStub().setProfile(request);

    assertTrue(response.hasExpectedVersionWriteConflict());
    verify(profilesManager, never()).set(any(), any(), any(), any());
    verify(accountsManager, never()).updateCurrentProfileVersion(any(), any(), any(), any());
  }

  @Test
  void setProfileExpectedVersionWriteConflict() throws Exception {
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AUTHENTICATED_ACI)).serialize();
    final byte[] validAboutEmoji = new byte[60];
    final byte[] validAbout = new byte[540];
    final byte[] validPaymentAddress = new byte[582];
    final byte[] validPhoneNumberSharing = new byte[29];

    doThrow(WriteConflictException.class)
        .when(accountsManager).updateCurrentProfileVersion(any(), any(), any(), any());

    final SetProfileRequest request = SetProfileRequest.newBuilder()
        .setVersion(ByteString.copyFrom(VERSION))
        .setData(ByteString.copyFrom(VALID_DATA))
        .setCommitment(ByteString.copyFrom(commitment)) // after v1 -> v2 migration, commitment can be null if expectedDataHash is present
        .setPaymentAddress(ByteString.copyFrom(validPaymentAddress))
        .setV1Request(V1_REQUEST.toBuilder()
            .setAvatarChange(AvatarChange.AVATAR_CHANGE_UNCHANGED)
            .setAboutEmoji(ByteString.copyFrom(validAboutEmoji))
            .setAbout(ByteString.copyFrom(validAbout))
            .setPhoneNumberSharing(ByteString.copyFrom(validPhoneNumberSharing))
        )
        .build();

    final SetProfileResponse response = authenticatedServiceStub().setProfile(request);

    assertTrue(response.hasExpectedVersionWriteConflict());
    verify(profilesManager).set(any(), any(), any(), any());
    verify(accountsManager).updateCurrentProfileVersion(any(), any(), any(), any());
  }

  @Test
  void setProfileWithoutCapability() {
    when(account.hasCapability(DeviceCapability.PROFILES_V2)).thenReturn(false);

    final SetProfileRequest request = SetProfileRequest.newBuilder()
        .setVersion(ByteString.copyFrom(VERSION))
        .setData(ByteString.copyFrom(VALID_DATA))
        .setExpectedCurrentDataHash(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
        .setV1Request(V1_REQUEST)
        .build();

    final SetProfileResponse response = authenticatedServiceStub().setProfile(request);

    assertTrue(response.hasProfilesV2CapabilityRequired());
  }

  @ParameterizedTest
  @MethodSource
  void setProfileUpload(final AvatarChange avatarChange, final boolean hasPreviousProfile,
      final boolean expectHasS3UploadPath, final boolean expectDeleteS3Object) throws InvalidInputException {
    final String currentAvatar = "profiles/currentAvatar";
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AUTHENTICATED_ACI)).serialize();

    final SetProfileRequest request = SetProfileRequest.newBuilder()
        .setVersion(ByteString.copyFrom(VERSION))
        .setData(ByteString.copyFrom(VALID_DATA))
        .setCommitment(ByteString.copyFrom(commitment))
        .setV1Request(V1_REQUEST.toBuilder().setAvatarChange(avatarChange).build())
        .build();

    when(profile.avatar()).thenReturn(currentAvatar);

    when(profilesManager.getV1(any(), anyString())).thenReturn(hasPreviousProfile ? Optional.of(profile) : Optional.empty());

    final SetProfileResponse response = authenticatedServiceStub().setProfile(request);

    assertTrue(response.hasResult());
    final SetProfileResult result = response.getResult();

    if (expectHasS3UploadPath) {
      assertTrue(result.getV1AvatarUploadForm().getKey().startsWith("profiles/"));
    } else {
      assertEquals("", result.getV1AvatarUploadForm().getKey());
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
  void setProfileInvalidRequestData(final SetProfileRequest request, final String invalidFieldOrMessageFragment, final boolean assertFieldViolation) {
    assertStatusInvalidArgument(() -> authenticatedServiceStub().setProfile(request));
    // com.google.rpc.Status.parseFrom(exception.getTrailers().valueAsBytes(0)).getDetailsList().get(1).unpack(BadRequest.class).getFieldViolationsList().getFirst().getField()
    final StatusRuntimeException e = assertStatusInvalidArgument(() -> authenticatedServiceStub().setProfile(request));
    if (assertFieldViolation) {
      final List<BadRequest.FieldViolation> fieldViolations = GrpcTestUtils.extractDetail(BadRequest.class, e)
          .getFieldViolationsList();

      assertEquals(1, fieldViolations.size());
      assertEquals(invalidFieldOrMessageFragment, fieldViolations.getFirst().getField());
    } else {
      assertThat(e.getMessage()).contains(invalidFieldOrMessageFragment);
    }
  }

  private static Stream<Arguments> setProfileInvalidRequestData() throws InvalidInputException{
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AuthHelper.VALID_UUID_TWO)).serialize();
    final byte[] invalidValue = new byte[42];

    final SetProfileRequest prototypeRequest = SetProfileRequest.newBuilder()
        .setVersion(ByteString.copyFrom(VERSION))
        .setData(ByteString.copyFrom(VALID_DATA))
        .setCommitment(ByteString.copyFrom(commitment))
        .setV1Request(V1_REQUEST)
        .build();

    return Stream.of(
        Arguments.argumentSet("Missing version", SetProfileRequest.newBuilder(prototypeRequest)
            .clearVersion()
            .build(), "version", true),
        Arguments.argumentSet("Invalid current data hash", SetProfileRequest.newBuilder(prototypeRequest)
            .setExpectedCurrentDataHash(ByteString.copyFrom(invalidValue))
            .build(), "expected_current_data_hash", true),
        Arguments.argumentSet("Invalid current version", SetProfileRequest.newBuilder(prototypeRequest)
            .setExpectedCurrentVersion(ByteString.copyFrom(new byte[1]))
            .build(), "expected_current_version", true),
        Arguments.argumentSet("Missing name", SetProfileRequest.newBuilder(prototypeRequest)
            .setV1Request(prototypeRequest.getV1Request().toBuilder().clearName())
            .build(), "name", true),
        Arguments.argumentSet("Invalid name length", SetProfileRequest.newBuilder(prototypeRequest)
            .setV1Request(prototypeRequest.getV1Request().toBuilder()
                .setName(ByteString.copyFrom(invalidValue))
                .build())
            .build(), "name", true),
        Arguments.argumentSet("Invalid about emoji length", SetProfileRequest.newBuilder(prototypeRequest)
            .setV1Request(prototypeRequest.getV1Request().toBuilder()
                .setAboutEmoji(ByteString.copyFrom(invalidValue)))
            .build(), "about_emoji", true),
        Arguments.argumentSet("Invalid about length", SetProfileRequest.newBuilder(prototypeRequest)
            .setV1Request(prototypeRequest.getV1Request().toBuilder()
                .setAbout(ByteString.copyFrom(invalidValue)))
            .build(), "about", true),
        Arguments.argumentSet("Invalid payment address", SetProfileRequest.newBuilder(prototypeRequest)
            .setPaymentAddress(ByteString.copyFrom(invalidValue))
            .build(), "payment_address", true),
        Arguments.argumentSet("Missing v1 request", SetProfileRequest.newBuilder(prototypeRequest)
            .clearV1Request()
            .build(), "v1Request", true),

        // this is an invalid request but not a field validation
        Arguments.argumentSet("Missing profile commitment", SetProfileRequest.newBuilder(prototypeRequest)
            .clearCommitment()
            .build(), "commitment", false)
    );
  }

  @ParameterizedTest
  @MethodSource
  void setPaymentAddressDisallowedCountry(@Nullable final byte[] existingPaymentAddress, final byte[] requestPaymentAddress, final boolean expectAllowed) throws InvalidInputException {
    final Phonenumber.PhoneNumber disallowedPhoneNumber = PhoneNumberUtil.getInstance().getExampleNumber("CU");
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AUTHENTICATED_ACI)).serialize();

    when(profile.paymentAddress()).thenReturn(existingPaymentAddress);

    final SetProfileRequest request = SetProfileRequest.newBuilder()
        .setVersion(ByteString.copyFrom(VERSION))
        .setData(ByteString.copyFrom(VALID_DATA))
        .setPaymentAddress(ByteString.copyFrom(requestPaymentAddress))
        .setCommitment(ByteString.copyFrom(commitment))
        .setV1Request(V1_REQUEST)
        .build();
    final String disallowedCountryCode = String.format("+%d", disallowedPhoneNumber.getCountryCode());
    when(dynamicPaymentsConfiguration.getDisallowedPrefixes()).thenReturn(List.of(disallowedCountryCode));
    when(account.getNumber()).thenReturn(PhoneNumberUtil.getInstance().format(
        disallowedPhoneNumber,
        PhoneNumberUtil.PhoneNumberFormat.E164));
    when(profilesManager.getV1(any(), anyString())).thenReturn(Optional.of(profile));

    final SetProfileResponse response = authenticatedServiceStub().setProfile(request);

    if (expectAllowed) {
      assertTrue(response.hasResult());
    } else {
      assertTrue(response.hasPaymentsForbiddenInRegion());
    }
  }

  static Collection<Arguments> setPaymentAddressDisallowedCountry() {
    return List.of(
        Arguments.argumentSet("null existing address, zero-length new", null, new byte[0], true),
        Arguments.argumentSet("null existing address", null, TestRandomUtil.nextBytes(582), false),
        Arguments.argumentSet("zero-length existing address, zero-length new", new byte[0], new byte[0], true),
        Arguments.argumentSet("zero-length existing address", new byte[0], TestRandomUtil.nextBytes(582), false),
        Arguments.argumentSet("valid existing address", TestRandomUtil.nextBytes(582), TestRandomUtil.nextBytes(582), true)
    );
  }

  @Test
  void setProfileBadges() throws Exception {

    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AUTHENTICATED_ACI)).serialize();

    final SetProfileRequest request = SetProfileRequest.newBuilder()
        .setVersion(ByteString.copyFrom(VERSION))
        .setData(ByteString.copyFrom(VALID_DATA))
        .addAllBadgeIds(List.of("TEST3"))
        .setCommitment(ByteString.copyFrom(commitment))
        .setV1Request(V1_REQUEST)
        .build();

    final int accountsManagerUpdateRetryCount = 2;
    setUpMockUpdateWithRetries(accountsManager, accountsManagerUpdateRetryCount);
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

  @Test
  void setProfileBadgesEmptyListClearsAll() throws InvalidInputException {
    final byte[] commitment = new ProfileKey(new byte[32]).getCommitment(new ServiceId.Aci(AUTHENTICATED_ACI)).serialize();

    final SetProfileRequest request = SetProfileRequest.newBuilder()
        .setVersion(ByteString.copyFrom(VERSION))
        .setData(ByteString.copyFrom(VALID_DATA))
        .setCommitment(ByteString.copyFrom(commitment))
        .setV1Request(V1_REQUEST)
        .build();

    final List<AccountBadge> existingBadges = List.of(
        new AccountBadge("TEST1", Instant.ofEpochSecond(41), true),
        new AccountBadge("TEST2", Instant.ofEpochSecond(41), true));
    when(account.getBadges()).thenReturn(existingBadges);

    setUpMockUpdateWithRetries(accountsManager, 1);

    authenticatedServiceStub().setProfile(request);

    @SuppressWarnings("unchecked")
    final ArgumentCaptor<List<AccountBadge>> badgeCaptor = ArgumentCaptor.forClass(List.class);
    verify(account).setBadges(refEq(clock), badgeCaptor.capture());

    assertEquals(List.of(
        new AccountBadge("TEST1", Instant.ofEpochSecond(41), false),
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
    final GetUnversionedProfileResult prototypeExpectedResult = GetUnversionedProfileResult.newBuilder()
        .setIdentityKey(ByteString.copyFrom(identityKey.serialize()))
        .setUnidentifiedAccess(ByteString.copyFrom(unidentifiedAccessChecksum))
        .setUnrestrictedUnidentifiedAccess(true)
        .addAllBadges(badges.stream().map(BadgeGrpcHelper::toGrpcBadge).toList())
        .build();

    final GetUnversionedProfileResult expectedResponse;
    if (identityType == IdentityType.IDENTITY_TYPE_PNI) {
      expectedResponse = GetUnversionedProfileResult.newBuilder(prototypeExpectedResult)
          .clearUnidentifiedAccess()
          .clearBadges()
          .setUnrestrictedUnidentifiedAccess(false)
          .build();
    } else {
      expectedResponse = prototypeExpectedResult;
    }

    verify(rateLimiter).validate(AUTHENTICATED_ACI);
    verify(accountsManager).getByServiceIdentifier(targetIdentifier);

    assertTrue(response.hasResult());
    assertEquals(expectedResponse, response.getResult());
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

    final GetUnversionedProfileResponse response = authenticatedServiceStub().getUnversionedProfile(request);
    assertTrue(response.hasNotFound());
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
  void getVersionedProfile(final byte[] requestVersion, @Nullable final byte[] accountVersion, final boolean expectResponseHasPaymentAddress) {
    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] emoji = TestRandomUtil.nextBytes(60);
    final byte[] about = TestRandomUtil.nextBytes(156);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final byte[] phoneNumberSharing = TestRandomUtil.nextBytes(29);
    final String avatar = "profiles/" + ProfileTestHelper.generateRandomBase64FromByteArray(16);

    final Optional<VersionedProfileV1> profile = Optional.of(new VersionedProfileV1(HexFormat.of().formatHex(requestVersion), name, avatar, emoji, about, paymentAddress,
        phoneNumberSharing, new byte[0]));

    final GetVersionedProfileRequest request = GetVersionedProfileRequest.newBuilder()
        .setAccountIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
            .build())
        .setVersion(ByteString.copyFrom(requestVersion))
        .build();

    when(account.getCurrentProfileVersion()).thenReturn(Optional.ofNullable(accountVersion));
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.of(account));
    when(profilesManager.getV1(any(), any())).thenReturn(profile);

    final GetVersionedProfileResponse response = authenticatedServiceStub().getVersionedProfile(request);

    final GetVersionedProfileResult.Builder expectedResultBuilder = GetVersionedProfileResult.newBuilder()
        .setV1Response(GetVersionedProfileV1Response.newBuilder()
            .setName(ByteString.copyFrom(name))
            .setAbout(ByteString.copyFrom(about))
            .setAboutEmoji(ByteString.copyFrom(emoji))
            .setAvatar(avatar)
            .setPhoneNumberSharing(ByteString.copyFrom(phoneNumberSharing))
            .build());

    if (expectResponseHasPaymentAddress) {
      expectedResultBuilder.setPaymentAddressDataEtag(
          DataEtag.newBuilder().setData(ByteString.copyFrom(paymentAddress))
              .setEtagSha256(ByteString.copyFrom(ProfileGrpcHelper.hash(paymentAddress)))
              .build());

    }

    assertTrue(response.hasResult());
    assertEquals(expectedResultBuilder.build(), response.getResult());
  }
  private static Collection<Arguments> getVersionedProfile() {
    final byte[] version1 = TestRandomUtil.nextBytes(32);
    final byte[] version2 = TestRandomUtil.nextBytes(32);
    return List.of(
        Arguments.argumentSet("current profile version matches", version1, version1, true),
        Arguments.argumentSet("current profile version absent",version1, null, true),
        Arguments.argumentSet("current profile version mismatch",version1, version2, false)
    );
  }

  @Test
  void getVersionedProfileV2() {
    final byte[] data = TestRandomUtil.nextBytes(128);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final byte[] commitment = TestRandomUtil.nextBytes(97);
    final byte[] version = TestRandomUtil.nextBytes(32);

    final UUID targetAci = UUID.randomUUID();
    final VersionedProfile v2Profile = new VersionedProfile(version, data, paymentAddress, commitment);

    final Account targetAccount = mock(Account.class);
    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(targetAci)))
        .thenReturn(Optional.of(targetAccount));
    when(targetAccount.getUuid()).thenReturn(targetAci);
    when(targetAccount.getCurrentProfileVersion()).thenReturn(Optional.of(version));
    when(targetAccount.hasCapability(DeviceCapability.PROFILES_V2)).thenReturn(true);

    when(profilesManager.get(eq(targetAci), aryEq(version))).thenReturn(Optional.of(v2Profile));
    when(profilesManager.getV1(any(), any())).thenReturn(Optional.empty());

    final GetVersionedProfileRequest request = GetVersionedProfileRequest.newBuilder()
        .setAccountIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(targetAci)))
            .build())
        .setVersion(ByteString.copyFrom(version))
        .build();

    final GetVersionedProfileResponse response = authenticatedServiceStub().getVersionedProfile(request);
    assertTrue(response.hasResult());
    final GetVersionedProfileResult result = response.getResult();

    assertTrue(result.hasDataEtag());
    assertEquals(ByteString.copyFrom(data), result.getDataEtag().getData());
    assertEquals(ByteString.copyFrom(v2Profile.dataHash()), result.getDataEtag().getEtagSha256());
    assertTrue(result.hasPaymentAddressDataEtag());
    assertEquals(ByteString.copyFrom(paymentAddress), result.getPaymentAddressDataEtag().getData());
    assertEquals(ByteString.copyFrom(v2Profile.paymentAddressHash()), result.getPaymentAddressDataEtag().getEtagSha256());
    assertFalse(result.getDataEtagMatched());
    assertFalse(result.getPaymentAddressEtagMatched());
    assertFalse(result.hasV1Response());
  }

  @Test
  void getVersionedProfileV2EtagMatched() {
    final byte[] data = TestRandomUtil.nextBytes(128);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final byte[] commitment = TestRandomUtil.nextBytes(97);
    final byte[] version = TestRandomUtil.nextBytes(32);
    final VersionedProfile v2Profile = new VersionedProfile(version, data, paymentAddress, commitment);

    final UUID targetAci = UUID.randomUUID();
    final Account targetAccount = mock(Account.class);
    when(targetAccount.getUuid()).thenReturn(targetAci);
    when(targetAccount.hasCapability(DeviceCapability.PROFILES_V2)).thenReturn(true);
    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(targetAci))).thenReturn(Optional.of(targetAccount));

    when(targetAccount.getCurrentProfileVersion()).thenReturn(Optional.of(version));
    when(profilesManager.get(targetAci, version)).thenReturn(Optional.of(v2Profile));
    when(profilesManager.getV1(any(), any())).thenReturn(Optional.empty());

    final GetVersionedProfileRequest.Builder builder = GetVersionedProfileRequest.newBuilder()
        .setAccountIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(targetAci)))
            .build())
        .setVersion(ByteString.copyFrom(version))
        .setDataEtag(ByteString.copyFrom(v2Profile.dataHash()));

    if (v2Profile.paymentAddressHash() != null) {
      builder.setPaymentAddressEtag(ByteString.copyFrom(v2Profile.paymentAddressHash()));
    }

    final GetVersionedProfileRequest request = builder.build();

    final GetVersionedProfileResponse response = authenticatedServiceStub().getVersionedProfile(request);
    assertTrue(response.hasResult());
    final GetVersionedProfileResult result = response.getResult();

    assertTrue(result.getDataEtagMatched());
    assertTrue(result.getPaymentAddressEtagMatched());
    assertFalse(result.hasDataEtag());
    assertFalse(result.hasPaymentAddressDataEtag());
    assertFalse(result.hasV1Response());
  }

  @ParameterizedTest
  @MethodSource
  void getVersionedProfileAccountOrProfileNotFound(final boolean missingAccount, final boolean missingProfile) {
    final GetVersionedProfileRequest request = GetVersionedProfileRequest.newBuilder()
        .setAccountIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
            .build())
        .setVersion(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
        .build();
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(missingAccount ? Optional.empty() : Optional.of(account));
    when(profilesManager.getV1(any(), any())).thenReturn(missingProfile ? Optional.empty() : Optional.of(profile));

    final GetVersionedProfileResponse response = authenticatedServiceStub().getVersionedProfile(request);
    assertTrue(response.hasNotFound());
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
        .setVersion(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
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
        .setVersion(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
        .build();

    assertStatusException(Status.INVALID_ARGUMENT, () -> authenticatedServiceStub().getVersionedProfile(request));
  }

  @Test
  void getAvatarCredential() {
    final ZkCredentialKeyPair zkCredentialKeyPair = ZkCredentialKeyPair.generate();
    final long rotationId = 617L;

    // this test needs a valid clock
    clock.setTimeMillis(Instant.now().toEpochMilli());

    when(account.getZkCredentialKey()).thenReturn(Optional.of(zkCredentialKeyPair.getPublicKey()));
    when(account.getZkCredentialKeyRotationId()).thenReturn(rotationId);

    final AvatarUploadCredentialRequestContext avatarUploadCredentialRequestContext = AvatarUploadCredentialRequestContext.create(
        new ServiceId.Aci(AUTHENTICATED_ACI), zkCredentialKeyPair, rotationId);
    final AvatarUploadCredentialRequest credentialRequest = avatarUploadCredentialRequestContext.getRequest();

    final GetAvatarCredentialsResponse response = authenticatedServiceStub().getAvatarCredentials(
        GetAvatarCredentialsRequest.newBuilder()
            .setAvatarCredentialsRequest(ByteString.copyFrom(credentialRequest.serialize()))
            .build());

    assertTrue(response.hasAvatarCredentials());

    assertDoesNotThrow(() -> avatarUploadCredentialRequestContext.receiveResponse(
        new AvatarUploadCredentialResponse(response.getAvatarCredentials().toByteArray()),
        genericServerSecretParams.getPublicParams()));
  }

  @Test
  void getAvatarCredentialAccountNotFound() {
    when(accountsManager.getByAccountIdentifier(AUTHENTICATED_ACI)).thenReturn(Optional.empty());

    final StatusRuntimeException statusRuntimeException = assertStatusException(Status.UNAUTHENTICATED,
        () -> authenticatedServiceStub().getAvatarCredentials(GetAvatarCredentialsRequest.getDefaultInstance()));
    assertEquals("account not found", statusRuntimeException.getStatus().getDescription());
  }

  @Test
  void getAvatarCredentialMissingZkCredentialKey() {
    when(account.getZkCredentialKey()).thenReturn(Optional.empty());

    final GetAvatarCredentialsResponse response = authenticatedServiceStub().getAvatarCredentials(
        GetAvatarCredentialsRequest.getDefaultInstance());

    assertTrue(response.hasMissingZkCredentialKey());

    assertEquals("account requires ZK credential key", response.getMissingZkCredentialKey().getDescription());
  }

  @Test
  void getAvatarCredentialInvalidCredentialRequest() {
    final ZkCredentialKeyPair zkCredentialKeyPair = ZkCredentialKeyPair.generate();
    final long rotationId = 617L;
    final long incorrectRotationId = rotationId + 1;

    // this test needs a valid clock
    clock.setTimeMillis(Instant.now().toEpochMilli());

    when(account.getZkCredentialKey()).thenReturn(Optional.of(zkCredentialKeyPair.getPublicKey()));
    when(account.getZkCredentialKeyRotationId()).thenReturn(rotationId);

    final AvatarUploadCredentialRequestContext avatarUploadCredentialRequestContext = AvatarUploadCredentialRequestContext.create(
        new ServiceId.Aci(AUTHENTICATED_ACI), zkCredentialKeyPair, incorrectRotationId);
    final AvatarUploadCredentialRequest credentialRequest = avatarUploadCredentialRequestContext.getRequest();

    final StatusRuntimeException statusRuntimeException = assertStatusInvalidArgument(
        () -> authenticatedServiceStub().getAvatarCredentials(
            GetAvatarCredentialsRequest.newBuilder()
                .setAvatarCredentialsRequest(ByteString.copyFrom(credentialRequest.serialize()))
                .build()));

    assertEquals("invalid credential request", statusRuntimeException.getStatus().getDescription());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 16, 32})
  void testProfileSerialized(int aboutEmojiLength) {

    // This test demonstrates the calculation for the (require.size).max = 909 + 28
    // in profiles.proto, as well as how padding calculation results in a constant size.
    //
    // This should be largely integrated into libsignal in the future.

    // in gRPC, field + varint length encoding has:
    // field number: 1 byte per field < number 16
    // varint: 1 byte for length < 128, 2 bytes for < 16,384
    final int serializedSize =
        1 + 257 + 2 +
        1 + 512 + 2 +
        1 + aboutEmojiLength +  1 +
        1 + 64 + 1 +
        1 + 1 +
        1 + (64 - (257 + 512 + aboutEmojiLength + 64 + 1) % 64) + 1;

    assertEquals(909, serializedSize);

    final PlaintextProfileData data = PlaintextProfileData.newBuilder()
        .setNameBytes(ByteString.copyFrom(new byte[257]))
        .setAboutBytes(ByteString.copyFrom(new byte[512]))
        .setAboutEmojiBytes(ByteString.copyFrom(new byte[aboutEmojiLength]))
        .setAvatarBytes(ByteString.copyFrom(new byte[64]))
        .setPhoneNumberSharing(true)
        .setPadding(ByteString.copyFrom(new byte[64 - (257 + 512 + aboutEmojiLength + 64 + 1) % 64]))
        .build();

    assertEquals(serializedSize, data.getSerializedSize());
  }

  static void setUpMockUpdateWithRetries(final AccountsManager accountsManager, final int retryCount) {
    try {
      when(accountsManager.updateCurrentProfileVersion(any(), any(), any(), any()))
          .thenAnswer(a -> {
            final Account account = accountsManager.getByAccountIdentifier(a.getArgument(0, UUID.class)).orElseThrow();
            for (int i = 0; i < retryCount; i++) {
              //noinspection unchecked
              a.getArgument(3, Consumer.class).accept(account);
            }

            return account;
          });
    } catch (final WriteConflictException e) {
      throw new RuntimeException(e);
    }
  }
}
