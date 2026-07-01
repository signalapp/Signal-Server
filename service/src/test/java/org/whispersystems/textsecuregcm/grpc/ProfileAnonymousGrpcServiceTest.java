/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.assertj.core.api.AssertionsForClassTypes.assertThatNoException;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertRateLimitExceeded;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertStatusException;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertStatusInvalidArgument;

import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;
import com.google.rpc.BadRequest;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.signal.chat.common.IdentityType;
import org.signal.chat.common.ServiceIdentifier;
import org.signal.chat.profile.CredentialType;
import org.signal.chat.profile.DeleteAvatarRequest;
import org.signal.chat.profile.DeleteAvatarResponse;
import org.signal.chat.profile.ExtendAvatarTTLRequest;
import org.signal.chat.profile.ExtendAvatarTTLResponse;
import org.signal.chat.profile.GetAvatarUploadFormRequest;
import org.signal.chat.profile.GetAvatarUploadFormResponse;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialAnonymousRequest;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialAnonymousResponse;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialRequest;
import org.signal.chat.profile.GetProfileAnonymousRequest;
import org.signal.chat.profile.GetProfileAnonymousResponse;
import org.signal.chat.profile.GetProfileRequest;
import org.signal.chat.profile.ProfileAnonymousGrpc;
import org.signal.chat.profile.ProfileResult;
import org.signal.chat.profile.LegacyProfileResult;
import org.signal.chat.profile.AccountInfo;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.signal.libsignal.zkgroup.GenericServerSecretParams;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.ZkCredentialKeyPair;
import org.signal.libsignal.zkgroup.avatars.AvatarUploadCredentialPresentation;
import org.signal.libsignal.zkgroup.avatars.AvatarUploadCredentialRequestContext;
import org.signal.libsignal.zkgroup.avatars.AvatarUploadCredentialResponse;
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
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.storage.VersionedProfileV1;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.ProfileTestHelper;
import org.whispersystems.textsecuregcm.util.MutableClock;
import org.whispersystems.textsecuregcm.util.ProfileHelper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class ProfileAnonymousGrpcServiceTest extends SimpleBaseGrpcTest<ProfileAnonymousGrpcService, ProfileAnonymousGrpc.ProfileAnonymousBlockingStub> {

  private static final ServerSecretParams SERVER_SECRET_PARAMS = ServerSecretParams.generate();

  @Mock
  private Account account;

  @Mock
  private AccountsManager accountsManager;

  @Mock
  private ProfilesManager profilesManager;

  @Mock
  private ProfileBadgeConverter profileBadgeConverter;

  @Mock
  private RateLimiter profileAvatarBytesRateLimiter;

  private ServerZkProfileOperations zkProfileOperations;

  private final GenericServerSecretParams genericServerSecretParams = GenericServerSecretParams.generate();

  private MutableClock clock;

  @Override
  protected ProfileAnonymousGrpcService createServiceBeforeEachTest() {
    getMockRequestAttributesInterceptor().setRequestAttributes(new RequestAttributes(InetAddresses.forString("127.0.0.1"),
        "Signal-Android/1.2.3",
        "en-us"));

    zkProfileOperations = spy(new ServerZkProfileOperations(SERVER_SECRET_PARAMS));

    final RateLimiters rateLimiters = mock(RateLimiters.class);
    when(rateLimiters.getProfileAvatarBytesLimiter()).thenReturn(profileAvatarBytesRateLimiter);

    clock = new MutableClock();

    return new ProfileAnonymousGrpcService(
        accountsManager,
        profilesManager,
        profileBadgeConverter,
        new PostPolicyGenerator("us-west-1", "profile-bucket", "accessKey", "accessSecret"),
        genericServerSecretParams,
        rateLimiters,
        clock,
        zkProfileOperations,
        new GroupSendTokenUtil(SERVER_SECRET_PARAMS, clock)
    );
  }


  @ParameterizedTest
  @MethodSource
  void getVersionedProfile(final byte[] requestVersion,
      @Nullable final byte[] accountVersion,
      final boolean expectResponseHasPaymentAddress,
      final GetProfileAnonymousRequest.AuthenticationCase authenticationCase) throws Exception {
    final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    final ECKeyPair identityKeyPair = ECKeyPair.generate();
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);
    final List<String> badgeIds = List.of("TEST-BADGE");

    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] emoji = TestRandomUtil.nextBytes(60);
    final byte[] about = TestRandomUtil.nextBytes(156);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final byte[] phoneNumberSharing = TestRandomUtil.nextBytes(29);
    final String avatar = "profiles/" + ProfileTestHelper.generateRandomBase64FromByteArray(16);

    final Optional<VersionedProfileV1> profile = Optional.of(new VersionedProfileV1(HexFormat.of().formatHex(requestVersion), name, avatar, emoji, about, paymentAddress, phoneNumberSharing, new byte[0]));

    when(account.getCurrentProfileVersion()).thenReturn(Optional.ofNullable(accountVersion));
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);
    when(account.getIdentityKey(org.whispersystems.textsecuregcm.identity.IdentityType.ACI)).thenReturn(identityKey);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(account.getBadges()).thenReturn(Collections.emptyList());

    when(profileBadgeConverter.visibleBadgeIds(anyList())).thenReturn(badgeIds);
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.of(account));
    when(profilesManager.getV1(any(), any())).thenReturn(profile);

    final GetProfileAnonymousRequest.Builder requestBuilder = GetProfileAnonymousRequest.newBuilder()
        .setRequest(GetProfileRequest.newBuilder()
            .setAccountIdentifier(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier))
            .setVersion(ByteString.copyFrom(requestVersion)));
    switch (authenticationCase) {
      case GROUP_SEND_TOKEN -> {
        final Instant expiration = Instant.now().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.DAYS);
        final byte[] token = AuthHelper.validGroupSendToken(SERVER_SECRET_PARAMS, List.of(serviceIdentifier), expiration);
        requestBuilder.setGroupSendToken(ByteString.copyFrom(token));
      }
      case UNIDENTIFIED_ACCESS_KEY -> requestBuilder.setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey));
    }

    final GetProfileAnonymousResponse response = unauthenticatedServiceStub().getProfile(requestBuilder.build());

    final LegacyProfileResult.Builder expectedResultBuilder = LegacyProfileResult.newBuilder()
        .setName(ByteString.copyFrom(name))
        .setAbout(ByteString.copyFrom(about))
        .setAboutEmoji(ByteString.copyFrom(emoji))
        .setAvatar(avatar)
        .setPhoneNumberSharing(ByteString.copyFrom(phoneNumberSharing))
        .setAccountInfo(AccountInfo.newBuilder()
            .addAllBadgeIds(badgeIds)
            .setIdentityKey(ByteString.copyFrom(identityKey.serialize()))
            .setUnrestrictedUnidentifiedAccess(false)
            .setUnidentifiedAccessKeyFingerprint(ByteString.copyFrom(UnidentifiedAccessChecksum.generateFor(unidentifiedAccessKey))));

    if (expectResponseHasPaymentAddress) {
      expectedResultBuilder.setPaymentAddress(ByteString.copyFrom(paymentAddress));
    }

    assertTrue(response.hasProfileV1());
    assertEquals(expectedResultBuilder.build(), response.getProfileV1());
  }

  private static Collection<Arguments> getVersionedProfile() {
    final byte[] version1 = TestRandomUtil.nextBytes(32);
    final byte[] version2 = TestRandomUtil.nextBytes(32);
    return List.of(
        Arguments.argumentSet("current profile version matches, unidentified access key auth", version1, version1, true,
            GetProfileAnonymousRequest.AuthenticationCase.UNIDENTIFIED_ACCESS_KEY),
        Arguments.argumentSet("current profile version absent, unidentified access key auth",version1, null, true,
            GetProfileAnonymousRequest.AuthenticationCase.UNIDENTIFIED_ACCESS_KEY),
        Arguments.argumentSet("current profile version mismatch, unidentified access key auth",version1, version2, false,
            GetProfileAnonymousRequest.AuthenticationCase.UNIDENTIFIED_ACCESS_KEY),
        Arguments.argumentSet("current profile version matches, group send endorsement auth", version1, version1, true,
            GetProfileAnonymousRequest.AuthenticationCase.GROUP_SEND_TOKEN)
    );
  }

  @ParameterizedTest
  @EnumSource(mode = EXCLUDE, names = "AUTHENTICATION_NOT_SET")
  void getVersionedProfileV2(
      final GetProfileAnonymousRequest.AuthenticationCase authenticationCase) throws Exception {
    final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    final ECKeyPair identityKeyPair = ECKeyPair.generate();
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);
    final List<String> badgeIds = List.of("TEST-BADGE");

    final byte[] data = TestRandomUtil.nextBytes(128);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final byte[] commitment = TestRandomUtil.nextBytes(97);
    final byte[] version = TestRandomUtil.nextBytes(32);
    final VersionedProfile v2Profile = new VersionedProfile(version, data, paymentAddress, commitment);

    when(account.getCurrentProfileVersion()).thenReturn(Optional.of(version));
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(true);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(account.hasCapability(DeviceCapability.PROFILES_V2)).thenReturn(true);
    when(account.getIdentityKey(org.whispersystems.textsecuregcm.identity.IdentityType.ACI)).thenReturn(identityKey);
    when(account.getBadges()).thenReturn(Collections.emptyList());

    when(profileBadgeConverter.visibleBadgeIds(anyList())).thenReturn(badgeIds);
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.of(account));
    when(profilesManager.get(any(), any())).thenReturn(Optional.of(v2Profile));
    when(profilesManager.getV1(any(), any())).thenReturn(Optional.empty());

    final GetProfileAnonymousRequest.Builder requestBuilder = GetProfileAnonymousRequest.newBuilder()
        .setRequest(GetProfileRequest.newBuilder()
            .setAccountIdentifier(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier))
            .setVersion(ByteString.copyFrom(version)));
    switch (authenticationCase) {
      case GROUP_SEND_TOKEN -> {
        final Instant expiration = Instant.now().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.DAYS);
        final byte[] token = AuthHelper.validGroupSendToken(SERVER_SECRET_PARAMS, List.of(serviceIdentifier), expiration);
        requestBuilder.setGroupSendToken(ByteString.copyFrom(token));
      }
      case UNIDENTIFIED_ACCESS_KEY -> requestBuilder.setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey));
    }

    final GetProfileAnonymousResponse response = unauthenticatedServiceStub().getProfile(requestBuilder.build());
    assertTrue(response.hasProfile());

    final ProfileResult.Builder expectedBuilder = ProfileResult.newBuilder()
        .setData(ByteString.copyFrom(data))
        .setPaymentAddress(ByteString.copyFrom(paymentAddress))
        .setAccountInfo(AccountInfo.newBuilder()
            .addAllBadgeIds(badgeIds)
            .setIdentityKey(ByteString.copyFrom(identityKey.serialize()))
            .setUnrestrictedUnidentifiedAccess(true)
            .setUnidentifiedAccessKeyFingerprint(ByteString.copyFrom(UnidentifiedAccessChecksum.generateFor(unidentifiedAccessKey))));
    expectedBuilder.setEtag(ByteString.copyFrom(ProfileGrpcHelper.etag(expectedBuilder)));

    final ProfileResult result = response.getProfile();
    assertEquals(expectedBuilder.build(), result);
  }

  @Test
  void getVersionedProfileV2EtagMatched() {
    final ECKeyPair identityKeyPair = ECKeyPair.generate();
    final IdentityKey identityKey = new IdentityKey(identityKeyPair.getPublicKey());
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(
        UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);
    final List<String> badgeIds = List.of("TEST-BADGE");

    final byte[] data = TestRandomUtil.nextBytes(128);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final byte[] commitment = TestRandomUtil.nextBytes(97);
    final byte[] version = TestRandomUtil.nextBytes(32);
    final VersionedProfile v2Profile = new VersionedProfile(version, data, paymentAddress, commitment);

    when(account.getCurrentProfileVersion()).thenReturn(Optional.of(version));
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(true);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(account.hasCapability(DeviceCapability.PROFILES_V2)).thenReturn(true);
    when(account.getIdentityKey(org.whispersystems.textsecuregcm.identity.IdentityType.ACI)).thenReturn(identityKey);
    when(account.getBadges()).thenReturn(Collections.emptyList());

    when(profileBadgeConverter.visibleBadgeIds(anyList())).thenReturn(badgeIds);
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.of(account));
    when(profilesManager.get(any(), any())).thenReturn(Optional.of(v2Profile));
    when(profilesManager.getV1(any(), any())).thenReturn(Optional.empty());

    final byte[] expectedEtag = ProfileGrpcHelper.etag(ProfileResult.newBuilder()
        .setData(ByteString.copyFrom(data))
        .setPaymentAddress(ByteString.copyFrom(paymentAddress))
        .setAccountInfo(AccountInfo.newBuilder()
            .addAllBadgeIds(badgeIds)
            .setIdentityKey(ByteString.copyFrom(identityKey.serialize()))
            .setUnrestrictedUnidentifiedAccess(true)
            .setUnidentifiedAccessKeyFingerprint(ByteString.copyFrom(UnidentifiedAccessChecksum.generateFor(unidentifiedAccessKey)))));

    final GetProfileAnonymousRequest request = GetProfileAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetProfileRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
                .build())
            .setVersion(ByteString.copyFrom(version))
            .setEtag(ByteString.copyFrom(expectedEtag)))
        .build();

    final GetProfileAnonymousResponse response = unauthenticatedServiceStub().getProfile(request);
    assertTrue(response.hasEtagMatched());
    assertTrue(response.getEtagMatched());
  }

  @Test
  void getVersionedProfileVersionNotFound() {
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);

    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.of(account));
    when(profilesManager.getV1(any(), any())).thenReturn(Optional.empty());

    final GetProfileAnonymousRequest request = GetProfileAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetProfileRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
                .build())
            .setVersion(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .build())
        .build();

    final GetProfileAnonymousResponse response = unauthenticatedServiceStub().getProfile(
        request);

    assertTrue(response.hasNotFound());
  }

  @ParameterizedTest
  @EnumSource(mode = EXCLUDE, names = "AUTHENTICATION_NOT_SET")
  void getVersionedProfileAccountNotFound(
      final GetProfileAnonymousRequest.AuthenticationCase authenticationCase
  ) throws Exception {
    final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.empty());

    final GetProfileAnonymousRequest.Builder requestBuilder = GetProfileAnonymousRequest.newBuilder()
        .setRequest(GetProfileRequest.newBuilder()
            .setAccountIdentifier(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier))
            .setVersion(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .build());

    switch (authenticationCase) {
      case GROUP_SEND_TOKEN -> {
        final Instant expiration = Instant.now().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.DAYS);
        final byte[] token = AuthHelper.validGroupSendToken(SERVER_SECRET_PARAMS, List.of(serviceIdentifier), expiration);
        requestBuilder.setGroupSendToken(ByteString.copyFrom(token));
      }
      case UNIDENTIFIED_ACCESS_KEY -> requestBuilder.setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey));
    }

    final GetProfileAnonymousResponse response = unauthenticatedServiceStub().getProfile(requestBuilder.build());

   switch (authenticationCase) {
     case GROUP_SEND_TOKEN -> assertTrue(response.hasNotFound());
     case UNIDENTIFIED_ACCESS_KEY, AUTHENTICATION_NOT_SET -> assertTrue(response.hasFailedUnidentifiedAuthorization());
   }
  }

  @Test
  void getVersionedProfilePniInvalidArgument() {
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    final GetProfileAnonymousRequest request = GetProfileAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetProfileRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_PNI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
                .build())
            .setVersion(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .build())
        .build();

    //noinspection ThrowableNotThrown
    assertStatusException(Status.INVALID_ARGUMENT, () -> unauthenticatedServiceStub().getProfile(request));
  }

  @Test
  void getVersionedProfileNoAuth() {
    final GetProfileAnonymousRequest request = GetProfileAnonymousRequest.newBuilder()
        .setRequest(GetProfileRequest.newBuilder()
            .setAccountIdentifier(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(UUID.randomUUID()))))
        .build();

    //noinspection ThrowableNotThrown
    assertStatusException(Status.INVALID_ARGUMENT, () -> unauthenticatedServiceStub().getProfile(request));
  }

  @Test
  void getVersionedProfileIncorrectUnidentifiedAccessKey() {
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.of(account));

    final GetProfileAnonymousRequest request = GetProfileAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]))
        .setRequest(GetProfileRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
                .build())
            .setVersion(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .build())
        .build();

    final GetProfileAnonymousResponse response = unauthenticatedServiceStub().getProfile(request);

    assertTrue(response.hasFailedUnidentifiedAuthorization());
  }

  @ParameterizedTest
  @MethodSource
  void getVersionedProfileFailingGroupSendEndorsement(final boolean expired, final boolean wrongIdentifier) throws Exception {
    final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    final AciServiceIdentifier wrongServiceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

    when(accountsManager.getByServiceIdentifier(serviceIdentifier))
        .thenReturn(Optional.of(mock(Account.class)));

    final Instant expiration = expired ? Instant.now().truncatedTo(ChronoUnit.DAYS) : Instant.now().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.DAYS);

    final byte[] token = AuthHelper.validGroupSendToken(SERVER_SECRET_PARAMS,
        List.of(wrongIdentifier ? wrongServiceIdentifier : serviceIdentifier), expiration);

    final GetProfileAnonymousRequest request = GetProfileAnonymousRequest.newBuilder()
        .setGroupSendToken(ByteString.copyFrom(token))
        .setRequest(GetProfileRequest.newBuilder()
            .setAccountIdentifier(
                GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier))
            .setVersion(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .build())
        .build();

    final GetProfileAnonymousResponse response = unauthenticatedServiceStub().getProfile(request);

    assertTrue(response.hasFailedUnidentifiedAuthorization());
  }

  private static List<Arguments> getVersionedProfileFailingGroupSendEndorsement() {
    return List.of(
        Arguments.of(true, true),
        Arguments.of(true, false),
        Arguments.of(false, true)
    );
  }

  @Test
  void getExpiringProfileKeyCredential() throws Exception {
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);
    final UUID targetUuid = UUID.randomUUID();

    final ClientZkProfileOperations clientZkProfile = new ClientZkProfileOperations(SERVER_SECRET_PARAMS.getPublicParams());

    final byte[] profileKeyBytes = TestRandomUtil.nextBytes(32);
    final ProfileKey profileKey = new ProfileKey(profileKeyBytes);
    final ProfileKeyCommitment profileKeyCommitment = profileKey.getCommitment(new ServiceId.Aci(targetUuid));
    final ProfileKeyCredentialRequestContext profileKeyCredentialRequestContext =
        clientZkProfile.createProfileKeyCredentialRequestContext(new ServiceId.Aci(targetUuid), profileKey);

    final VersionedProfileV1 profile = mock(VersionedProfileV1.class);
    when(profile.commitment()).thenReturn(profileKeyCommitment.serialize());

    when(account.getUuid()).thenReturn(targetUuid);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(targetUuid))).thenReturn(Optional.of(account));
    when(profilesManager.getV1(targetUuid, HexFormat.of().formatHex(profileKeyBytes))).thenReturn(Optional.of(profile));

    final ProfileKeyCredentialRequest credentialRequest = profileKeyCredentialRequestContext.getRequest();

    final GetExpiringProfileKeyCredentialAnonymousRequest request = GetExpiringProfileKeyCredentialAnonymousRequest.newBuilder()
        .setRequest(GetExpiringProfileKeyCredentialRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(targetUuid)))
                .build())
            .setCredentialRequest(ByteString.copyFrom(credentialRequest.serialize()))
            .setCredentialType(CredentialType.CREDENTIAL_TYPE_EXPIRING_PROFILE_KEY)
            .setVersion(ByteString.copyFrom(profileKeyBytes))
            .build())
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .build();

    final GetExpiringProfileKeyCredentialAnonymousResponse response = unauthenticatedServiceStub().getExpiringProfileKeyCredential(request);

    final Instant expectedExpiration = Instant.now().plus(org.whispersystems.textsecuregcm.util.ProfileHelper.EXPIRING_PROFILE_KEY_CREDENTIAL_EXPIRATION)
        .truncatedTo(ChronoUnit.DAYS);
    verify(zkProfileOperations).issueExpiringProfileKeyCredential(eq(credentialRequest), eq(ServiceId.Aci.parseFromString(targetUuid.toString())), eq(profileKeyCommitment), eq(expectedExpiration));

    assertThatNoException().isThrownBy(() ->
        clientZkProfile.receiveExpiringProfileKeyCredential(profileKeyCredentialRequestContext,
            new ExpiringProfileKeyCredentialResponse(response.getResult().getProfileKeyCredential().toByteArray())));
  }

  @ParameterizedTest
  @MethodSource
  void getExpiringProfileKeyCredentialUnauthenticated(final boolean missingAccount, final boolean missingUnidentifiedAccessKey) {
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);
    final UUID targetUuid = UUID.randomUUID();

    when(account.getUuid()).thenReturn(targetUuid);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(targetUuid))).thenReturn(
        missingAccount ? Optional.empty() : Optional.of(account));

    final GetExpiringProfileKeyCredentialAnonymousRequest.Builder requestBuilder = GetExpiringProfileKeyCredentialAnonymousRequest.newBuilder()
        .setRequest(GetExpiringProfileKeyCredentialRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(targetUuid)))
                .build())
            .setCredentialRequest(ByteString.copyFrom("credentialRequest".getBytes(StandardCharsets.UTF_8)))
            .setCredentialType(CredentialType.CREDENTIAL_TYPE_EXPIRING_PROFILE_KEY)
            .setVersion(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .build());

    if (missingUnidentifiedAccessKey) {
      assertStatusException(Status.INVALID_ARGUMENT,
          () -> unauthenticatedServiceStub().getExpiringProfileKeyCredential(requestBuilder.build()));

    } else {
      requestBuilder.setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey));

      final GetExpiringProfileKeyCredentialAnonymousResponse response = unauthenticatedServiceStub().getExpiringProfileKeyCredential(
          requestBuilder.build());

      assertEquals(missingAccount, response.hasNotFound());
      assertNotEquals(missingAccount, response.hasFailedUnidentifiedAuthorization());
    }

    verifyNoInteractions(profilesManager);
  }

  private static Stream<Arguments> getExpiringProfileKeyCredentialUnauthenticated() {
    return Stream.of(
        Arguments.of(true, false),
        Arguments.of(false, true),
        Arguments.of(true, true)
    );
  }


  @Test
  void getExpiringProfileKeyCredentialProfileNotFound() {
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);
    final UUID targetUuid = UUID.randomUUID();

    final byte[] version = TestRandomUtil.nextBytes(32);
    final String versionHex = HexFormat.of().formatHex(version);

    when(account.getUuid()).thenReturn(targetUuid);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(targetUuid))).thenReturn(
        Optional.of(account));
    when(profilesManager.getV1(targetUuid, versionHex)).thenReturn(Optional.empty());
    when(profilesManager.get(eq(targetUuid), aryEq(version))).thenReturn(Optional.empty());

    final GetExpiringProfileKeyCredentialAnonymousRequest request = GetExpiringProfileKeyCredentialAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetExpiringProfileKeyCredentialRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(targetUuid)))
                .build())
            .setCredentialRequest(ByteString.copyFrom("credentialRequest".getBytes(StandardCharsets.UTF_8)))
            .setCredentialType(CredentialType.CREDENTIAL_TYPE_EXPIRING_PROFILE_KEY)
            .setVersion(ByteString.copyFrom(version))
            .build())
        .build();

    final GetExpiringProfileKeyCredentialAnonymousResponse response = unauthenticatedServiceStub().getExpiringProfileKeyCredential(
        request);

    assertTrue(response.hasNotFound());
  }

  @ParameterizedTest
  @MethodSource
  void getExpiringProfileKeyCredentialInvalidArgument(final IdentityType identityType, final CredentialType credentialType,
      final byte[] credentialRequest,
      final boolean throwZkVerificationException) throws VerificationFailedException {
    final UUID targetUuid = UUID.randomUUID();
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    final byte[] version = TestRandomUtil.nextBytes(32);
    final String versionHex = HexFormat.of().formatHex(version);

    final VersionedProfileV1 profile = mock(VersionedProfileV1.class);
    when(profile.commitment()).thenReturn("commitment".getBytes(StandardCharsets.UTF_8));
    when(account.getUuid()).thenReturn(targetUuid);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(accountsManager.getByServiceIdentifier(new AciServiceIdentifier(targetUuid))).thenReturn(Optional.of(account));
    when(profilesManager.getV1(targetUuid, versionHex)).thenReturn(Optional.of(profile));

    if (throwZkVerificationException) {
      doThrow(VerificationFailedException.class)
          .when(zkProfileOperations).issueExpiringProfileKeyCredential(any(), any(), any(), any());
    }

    final GetExpiringProfileKeyCredentialAnonymousRequest request = GetExpiringProfileKeyCredentialAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetExpiringProfileKeyCredentialRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(identityType)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(targetUuid)))
                .build())
            .setCredentialRequest(ByteString.copyFrom(credentialRequest))
            .setCredentialType(credentialType)
            .setVersion(ByteString.copyFrom(version))
            .build())
        .build();

    assertStatusException(Status.INVALID_ARGUMENT, () -> unauthenticatedServiceStub().getExpiringProfileKeyCredential(request));
  }

  private static Stream<Arguments> getExpiringProfileKeyCredentialInvalidArgument() throws Exception {
    final byte[] invalidCredentialRequest = "invalidRequest".getBytes();

    final UUID targetUuid = UUID.randomUUID();

    final ClientZkProfileOperations clientZkProfile = new ClientZkProfileOperations(SERVER_SECRET_PARAMS.getPublicParams());

    final byte[] profileKeyBytes = TestRandomUtil.nextBytes(32);

    final ProfileKey profileKey = new ProfileKey(profileKeyBytes);
    final ProfileKeyCommitment profileKeyCommitment = profileKey.getCommitment(new ServiceId.Aci(targetUuid));
    final ProfileKeyCredentialRequestContext profileKeyCredentialRequestContext =
        clientZkProfile.createProfileKeyCredentialRequestContext(new ServiceId.Aci(targetUuid), profileKey);
    final ProfileKeyCredentialRequest credentialRequest = profileKeyCredentialRequestContext.getRequest();

    final byte[] validCredentialRequest = credentialRequest.serialize();

    return Stream.of(
        Arguments.argumentSet("Credential type unspecified", IdentityType.IDENTITY_TYPE_ACI, CredentialType.CREDENTIAL_TYPE_UNSPECIFIED, validCredentialRequest, false),
        Arguments.argumentSet("Illegal identity type", IdentityType.IDENTITY_TYPE_PNI, CredentialType.CREDENTIAL_TYPE_EXPIRING_PROFILE_KEY, validCredentialRequest, false),
        Arguments.argumentSet("Invalid credential request", IdentityType.IDENTITY_TYPE_ACI, CredentialType.CREDENTIAL_TYPE_EXPIRING_PROFILE_KEY, invalidCredentialRequest, false),
        Arguments.argumentSet("ZK verification failure", IdentityType.IDENTITY_TYPE_ACI, CredentialType.CREDENTIAL_TYPE_EXPIRING_PROFILE_KEY, validCredentialRequest, true)
    );
  }

  @Test
  void getAvatarUploadForm() throws Exception {
    final AvatarUploadCredentialPresentation avatarUploadCredentialPresentation = getAvatarUploadCredentialPresentation(
        genericServerSecretParams, clock);

    final GetAvatarUploadFormRequest request = GetAvatarUploadFormRequest.newBuilder()
        .setAvatarCredentialsPresentation(ByteString.copyFrom(avatarUploadCredentialPresentation.serialize()))
        .setUploadLength(100)
        .build();

    final GetAvatarUploadFormResponse response = authenticatedServiceStub().getAvatarUploadForm(request);

    assertTrue(response.hasAvatarUploadForm());

    final String avatarPath = response.getAvatarUploadForm().getKey();
    assertFalse(avatarPath.isEmpty());

    verify(profilesManager).setAvatarForIdentity(aryEq(avatarUploadCredentialPresentation.getCommitment()), eq(avatarPath));
  }

  @Test
  void getAvatarUploadFormInvalidUploadLength() throws Exception {
    final AvatarUploadCredentialPresentation avatarUploadCredentialPresentation = getAvatarUploadCredentialPresentation(
        genericServerSecretParams, clock);

    final GetAvatarUploadFormRequest request = GetAvatarUploadFormRequest.newBuilder()
        .setAvatarCredentialsPresentation(ByteString.copyFrom(avatarUploadCredentialPresentation.serialize()))
        .setUploadLength(Math.toIntExact(ProfileHelper.MAX_PROFILE_AVATAR_SIZE_BYTES + 1))
        .build();

    final StatusRuntimeException statusRuntimeException = assertStatusInvalidArgument(
        () -> authenticatedServiceStub().getAvatarUploadForm(request));

    final List<BadRequest.FieldViolation> fieldViolations = GrpcTestUtils.extractDetail(BadRequest.class, statusRuntimeException)
        .getFieldViolationsList();

    assertEquals(1, fieldViolations.size());
    assertEquals("upload_length", fieldViolations.getFirst().getField());
  }

  @Test
  void getAvatarUploadFormInvalidCredentialsPresentation() throws Exception {
    final AvatarUploadCredentialPresentation avatarUploadCredentialPresentation = getAvatarUploadCredentialPresentation(
        genericServerSecretParams, clock);

    final GetAvatarUploadFormRequest request = GetAvatarUploadFormRequest.newBuilder()
        .setAvatarCredentialsPresentation(ByteString.copyFrom(avatarUploadCredentialPresentation.serialize()))
        .setUploadLength(100)
        .build();

    // trigger a verification failure by advancing the clock beyond validity
    clock.setTimeInstant(clock.instant().plus(Duration.ofDays(3)));

    final GetAvatarUploadFormResponse response = authenticatedServiceStub().getAvatarUploadForm(request);

    assertTrue(response.hasInvalidCredentialsPresentation());
  }

  @Test
  void getAvatarUploadFormRateLimited() throws Exception {
    final AvatarUploadCredentialPresentation avatarUploadCredentialPresentation = getAvatarUploadCredentialPresentation(
        genericServerSecretParams, clock);

    final Duration retryDuration = Duration.ofMinutes(20);
    doThrow(new RateLimitExceededException(retryDuration))
        .when(profileAvatarBytesRateLimiter).validate(anyString(), anyLong());

    final GetAvatarUploadFormRequest request = GetAvatarUploadFormRequest.newBuilder()
        .setAvatarCredentialsPresentation(ByteString.copyFrom(avatarUploadCredentialPresentation.serialize()))
        .setUploadLength(100)
        .build();

    assertRateLimitExceeded(retryDuration,
        () -> authenticatedServiceStub().getAvatarUploadForm(request),
        profilesManager);
  }

  @Test
  void extendAvatarTtl() throws Exception {
    final String path = "somePath";
    when(profilesManager.extendAvatarTtlForIdentity(any(byte[].class))).thenReturn(Optional.of(path));

    final AvatarUploadCredentialPresentation avatarUploadCredentialPresentation = getAvatarUploadCredentialPresentation(
        genericServerSecretParams, clock);

    final ExtendAvatarTTLRequest request = ExtendAvatarTTLRequest.newBuilder()
        .setAvatarCredentialsPresentation(ByteString.copyFrom(avatarUploadCredentialPresentation.serialize()))
        .build();

    final ExtendAvatarTTLResponse response = authenticatedServiceStub().extendAvatarTTL(request);

    assertTrue(response.hasPath());

    assertEquals(path, response.getPath());
  }

  @Test
  void extendAvatarTtlNoActiveAvatar() throws Exception {
    when(profilesManager.extendAvatarTtlForIdentity(any(byte[].class))).thenReturn(Optional.empty());

    final AvatarUploadCredentialPresentation avatarUploadCredentialPresentation = getAvatarUploadCredentialPresentation(
        genericServerSecretParams, clock);

    final ExtendAvatarTTLRequest request = ExtendAvatarTTLRequest.newBuilder()
        .setAvatarCredentialsPresentation(ByteString.copyFrom(avatarUploadCredentialPresentation.serialize()))
        .build();


    final ExtendAvatarTTLResponse response = authenticatedServiceStub().extendAvatarTTL(request);

    assertTrue(response.hasNotFound());
  }

  @Test
  void extendAvatarTtlInvalidCredentialsPresentation() throws Exception {
    final AvatarUploadCredentialPresentation avatarUploadCredentialPresentation = getAvatarUploadCredentialPresentation(
        genericServerSecretParams, clock);

    final ExtendAvatarTTLRequest request = ExtendAvatarTTLRequest.newBuilder()
        .setAvatarCredentialsPresentation(ByteString.copyFrom(avatarUploadCredentialPresentation.serialize()))
        .build();

    // trigger a verification failure by advancing the clock beyond validity
    clock.setTimeInstant(clock.instant().plus(Duration.ofDays(3)));

    final ExtendAvatarTTLResponse response = authenticatedServiceStub().extendAvatarTTL(request);

    assertTrue(response.hasInvalidCredentialsPresentation());
  }

  @Test
  void deleteAvatar() throws Exception {
    final AvatarUploadCredentialPresentation avatarUploadCredentialPresentation = getAvatarUploadCredentialPresentation(
        genericServerSecretParams, clock);

    final DeleteAvatarRequest request = DeleteAvatarRequest.newBuilder()
        .setAvatarCredentialsPresentation(ByteString.copyFrom(avatarUploadCredentialPresentation.serialize()))
        .build();

    final DeleteAvatarResponse response = authenticatedServiceStub().deleteAvatar(request);

    assertTrue(response.hasSuccess());

    verify(profilesManager).deleteAvatarForIdentity(aryEq(avatarUploadCredentialPresentation.getCommitment()));
  }

  @Test
  void deleteAvatarInvalidCredentialsPresentation() throws Exception {
    final AvatarUploadCredentialPresentation avatarUploadCredentialPresentation = getAvatarUploadCredentialPresentation(
        genericServerSecretParams, clock);

    final DeleteAvatarRequest request = DeleteAvatarRequest.newBuilder()
        .setAvatarCredentialsPresentation(ByteString.copyFrom(avatarUploadCredentialPresentation.serialize()))
        .build();

    // trigger a verification failure by advancing the clock beyond validity
    clock.setTimeInstant(clock.instant().plus(Duration.ofDays(3)));

    final DeleteAvatarResponse response = authenticatedServiceStub().deleteAvatar(request);

    assertTrue(response.hasInvalidCredentialsPresentation());
  }

  static AvatarUploadCredentialPresentation getAvatarUploadCredentialPresentation(final GenericServerSecretParams serverSecretParams, final Clock clock) throws VerificationFailedException {
    final ServiceId.Aci aci = new ServiceId.Aci(UUID.randomUUID());
    final ZkCredentialKeyPair zkCredentialKeyPair = ZkCredentialKeyPair.generate();
    final long rotationId = ThreadLocalRandom.current().nextLong();

    final AvatarUploadCredentialRequestContext context = AvatarUploadCredentialRequestContext.create(
        aci,
        zkCredentialKeyPair, rotationId);

    final AvatarUploadCredentialResponse avatarUploadCredentialResponse = context.getRequest()
        .issueCredential(aci, zkCredentialKeyPair.getPublicKey(), rotationId, clock.instant().truncatedTo(ChronoUnit.DAYS), serverSecretParams);

    return context.receiveResponse(avatarUploadCredentialResponse, clock.instant(),
        serverSecretParams.getPublicParams()).present(serverSecretParams.getPublicParams());
  }
}
