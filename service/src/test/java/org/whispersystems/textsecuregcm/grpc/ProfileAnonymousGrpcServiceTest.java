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
import static org.mockito.AdditionalMatchers.aryEq;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertStatusException;

import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Collections;
import java.util.HexFormat;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.signal.chat.common.IdentityType;
import org.signal.chat.common.ServiceIdentifier;
import org.signal.chat.profile.CredentialType;
import org.signal.chat.profile.DataEtag;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialAnonymousRequest;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialAnonymousResponse;
import org.signal.chat.profile.GetExpiringProfileKeyCredentialRequest;
import org.signal.chat.profile.GetUnversionedProfileAnonymousRequest;
import org.signal.chat.profile.GetUnversionedProfileAnonymousResponse;
import org.signal.chat.profile.GetUnversionedProfileRequest;
import org.signal.chat.profile.GetUnversionedProfileResult;
import org.signal.chat.profile.GetVersionedProfileAnonymousRequest;
import org.signal.chat.profile.GetVersionedProfileAnonymousResponse;
import org.signal.chat.profile.GetVersionedProfileRequest;
import org.signal.chat.profile.GetVersionedProfileResult;
import org.signal.chat.profile.GetVersionedProfileV1Response;
import org.signal.chat.profile.ProfileAnonymousGrpc;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
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
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.entities.BadgeSvg;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DeviceCapability;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.storage.VersionedProfileV1;
import org.whispersystems.textsecuregcm.tests.util.AuthHelper;
import org.whispersystems.textsecuregcm.tests.util.ProfileTestHelper;
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

  private ServerZkProfileOperations zkProfileOperations;

  @Override
  protected ProfileAnonymousGrpcService createServiceBeforeEachTest() {
    getMockRequestAttributesInterceptor().setRequestAttributes(new RequestAttributes(InetAddresses.forString("127.0.0.1"),
        "Signal-Android/1.2.3",
        "en-us"));

    zkProfileOperations = spy(new ServerZkProfileOperations(SERVER_SECRET_PARAMS));

    return new ProfileAnonymousGrpcService(
        accountsManager,
        profilesManager,
        profileBadgeConverter,
        zkProfileOperations,
        new GroupSendTokenUtil(SERVER_SECRET_PARAMS, Clock.systemUTC())
    );
  }

  @Test
  void getUnversionedProfileUnidentifiedAccessKey() {
    final UUID targetUuid = UUID.randomUUID();
    final org.whispersystems.textsecuregcm.identity.ServiceIdentifier serviceIdentifier = new AciServiceIdentifier(targetUuid);

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

    when(account.getBadges()).thenReturn(Collections.emptyList());
    when(profileBadgeConverter.convert(any(), any(), anyBoolean())).thenReturn(badges);
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(account.getIdentityKey(org.whispersystems.textsecuregcm.identity.IdentityType.ACI)).thenReturn(identityKey);
    when(account.hasCapability(any())).thenReturn(false);
    when(account.hasCapability(DeviceCapability.SPARSE_POST_QUANTUM_RATCHET)).thenReturn(true);
    when(accountsManager.getByServiceIdentifier(serviceIdentifier)).thenReturn(Optional.of(account));

    final GetUnversionedProfileAnonymousRequest request = GetUnversionedProfileAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetUnversionedProfileRequest.newBuilder()
            .setServiceIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(targetUuid)))
                .build())
            .build())
        .build();

    final GetUnversionedProfileAnonymousResponse response = unauthenticatedServiceStub().getUnversionedProfile(request);

    final byte[] unidentifiedAccessChecksum = UnidentifiedAccessChecksum.generateFor(unidentifiedAccessKey);
    final GetUnversionedProfileAnonymousResponse expectedResponse = GetUnversionedProfileAnonymousResponse.newBuilder()
        .setResult(GetUnversionedProfileResult.newBuilder()
            .setIdentityKey(ByteString.copyFrom(identityKey.serialize()))
            .setUnidentifiedAccess(ByteString.copyFrom(unidentifiedAccessChecksum))
            .setUnrestrictedUnidentifiedAccess(false)
            .addCapabilities(org.signal.chat.common.DeviceCapability.DEVICE_CAPABILITY_SPARSE_POST_QUANTUM_RATCHET)
            .addAllBadges(ProfileGrpcHelper.buildBadges(badges))
            .build()
        )
        .build();

    verify(accountsManager).getByServiceIdentifier(serviceIdentifier);
    assertEquals(expectedResponse, response);
  }

  @Test
  void getUnversionedProfileGroupSendEndorsement() throws Exception {
    final UUID targetUuid = UUID.randomUUID();
    final org.whispersystems.textsecuregcm.identity.ServiceIdentifier serviceIdentifier = new AciServiceIdentifier(targetUuid);

    // Expiration must be on a day boundary; we want one in the future
    final Instant expiration = Instant.now().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.DAYS);
    final byte[] token = AuthHelper.validGroupSendToken(SERVER_SECRET_PARAMS, List.of(serviceIdentifier), expiration);

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

    when(account.getBadges()).thenReturn(Collections.emptyList());
    when(profileBadgeConverter.convert(any(), any(), anyBoolean())).thenReturn(badges);
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);
    when(account.getIdentityKey(org.whispersystems.textsecuregcm.identity.IdentityType.ACI)).thenReturn(identityKey);
    when(accountsManager.getByServiceIdentifier(serviceIdentifier)).thenReturn(Optional.of(account));

    final GetUnversionedProfileAnonymousRequest request = GetUnversionedProfileAnonymousRequest.newBuilder()
        .setGroupSendToken(ByteString.copyFrom(token))
        .setRequest(GetUnversionedProfileRequest.newBuilder()
            .setServiceIdentifier(
                GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier))
            .build())
        .build();

    final GetUnversionedProfileAnonymousResponse response = unauthenticatedServiceStub().getUnversionedProfile(request);

    final GetUnversionedProfileResult expectedResult = GetUnversionedProfileResult.newBuilder()
            .setIdentityKey(ByteString.copyFrom(identityKey.serialize()))
            .setUnrestrictedUnidentifiedAccess(false)
            .addAllCapabilities(ProfileGrpcHelper.buildAccountCapabilities(account))
            .addAllBadges(ProfileGrpcHelper.buildBadges(badges))
            .build();

    verify(accountsManager).getByServiceIdentifier(serviceIdentifier);
    assertTrue(response.hasResult());
    assertEquals(expectedResult, response.getResult());
  }

  @Test
  void getUnversionedProfileNoAuth() {
    final GetUnversionedProfileAnonymousRequest request = GetUnversionedProfileAnonymousRequest.newBuilder()
        .setRequest(GetUnversionedProfileRequest.newBuilder()
            .setServiceIdentifier(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(new AciServiceIdentifier(UUID.randomUUID()))))
        .build();

    assertStatusException(Status.INVALID_ARGUMENT, () -> unauthenticatedServiceStub().getUnversionedProfile(request));
  }

  @ParameterizedTest
  @MethodSource
  void getUnversionedProfileIncorrectUnidentifiedAccessKey(final IdentityType identityType, final boolean wrongUnidentifiedAccessKey, final boolean accountNotFound) {
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(
        accountNotFound ? Optional.empty() : Optional.of(account));

    final GetUnversionedProfileAnonymousRequest request = GetUnversionedProfileAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(
            ByteString.copyFrom(wrongUnidentifiedAccessKey
                ? new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]
                : unidentifiedAccessKey))
        .setRequest(GetUnversionedProfileRequest.newBuilder()
            .setServiceIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(identityType)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))))
        .build();

    if (IdentityType.IDENTITY_TYPE_PNI.equals(identityType)) {
      assertStatusException(Status.INVALID_ARGUMENT, () -> unauthenticatedServiceStub().getUnversionedProfile(request));
    } else {

      final GetUnversionedProfileAnonymousResponse response = unauthenticatedServiceStub().getUnversionedProfile(request);

      assertTrue(response.hasFailedUnidentifiedAuthorization());
    }
  }

  private static Stream<Arguments> getUnversionedProfileIncorrectUnidentifiedAccessKey() {
    return Stream.of(
        Arguments.of(IdentityType.IDENTITY_TYPE_PNI, false, false),
        Arguments.of(IdentityType.IDENTITY_TYPE_ACI, true, false),
        Arguments.of(IdentityType.IDENTITY_TYPE_ACI, false, true)
    );
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void getUnversionedProfileExpiredGroupSendEndorsement(final boolean accountNotFound) throws Exception {
    final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    when(accountsManager.getByServiceIdentifier(serviceIdentifier))
        .thenReturn(accountNotFound ? Optional.empty() : Optional.of(mock(Account.class)));

    // Expirations must be on a day boundary; pick one in the recent past
    final Instant expiration = Instant.now().truncatedTo(ChronoUnit.DAYS);
    final byte[] token = AuthHelper.validGroupSendToken(SERVER_SECRET_PARAMS, List.of(serviceIdentifier), expiration);

    final GetUnversionedProfileAnonymousRequest request = GetUnversionedProfileAnonymousRequest.newBuilder()
        .setGroupSendToken(ByteString.copyFrom(token))
        .setRequest(GetUnversionedProfileRequest.newBuilder()
            .setServiceIdentifier(
                GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier)))
        .build();

    final GetUnversionedProfileAnonymousResponse response = unauthenticatedServiceStub().getUnversionedProfile(
        request);

    assertTrue(response.hasFailedUnidentifiedAuthorization());
  }

  @Test
  void getUnversionedProfileIncorrectGroupSendEndorsement() throws Exception {
    final AciServiceIdentifier targetServiceIdentifier = new AciServiceIdentifier(UUID.randomUUID());
    final AciServiceIdentifier authorizedServiceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

    // Expiration must be on a day boundary; we want one in the future
    final Instant expiration = Instant.now().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.DAYS);
    final byte[] token = AuthHelper.validGroupSendToken(SERVER_SECRET_PARAMS, List.of(authorizedServiceIdentifier), expiration);

    when(accountsManager.getByServiceIdentifier(any())).thenReturn(
        Optional.of(mock(Account.class)));
    final GetUnversionedProfileAnonymousRequest request = GetUnversionedProfileAnonymousRequest.newBuilder()
        .setGroupSendToken(ByteString.copyFrom(token))
        .setRequest(GetUnversionedProfileRequest.newBuilder()
            .setServiceIdentifier(
                GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(targetServiceIdentifier)))
        .build();

    final GetUnversionedProfileAnonymousResponse response = unauthenticatedServiceStub().getUnversionedProfile(request);

    assertTrue(response.hasFailedUnidentifiedAuthorization());
  }

  @Test
  void getUnversionedProfileGroupSendEndorsementAccountNotFound() throws Exception {
    final AciServiceIdentifier serviceIdentifier = new AciServiceIdentifier(UUID.randomUUID());

    // Expiration must be on a day boundary; we want one in the future
    final Instant expiration = Instant.now().plus(Duration.ofDays(1)).truncatedTo(ChronoUnit.DAYS);
    final byte[] token = AuthHelper.validGroupSendToken(SERVER_SECRET_PARAMS, List.of(serviceIdentifier), expiration);

    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.empty());
    final GetUnversionedProfileAnonymousRequest request = GetUnversionedProfileAnonymousRequest.newBuilder()
        .setGroupSendToken(ByteString.copyFrom(token))
        .setRequest(GetUnversionedProfileRequest.newBuilder()
            .setServiceIdentifier(GrpcServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifier)))
        .build();

    final GetUnversionedProfileAnonymousResponse response = unauthenticatedServiceStub().getUnversionedProfile(
        request);

    assertTrue(response.hasNotFound());
  }

  @ParameterizedTest
  @MethodSource
  void getVersionedProfile(final byte[] requestVersion,
      @Nullable final byte[] accountVersion,
      final boolean expectResponseHasPaymentAddress) {
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    final byte[] name = TestRandomUtil.nextBytes(81);
    final byte[] emoji = TestRandomUtil.nextBytes(60);
    final byte[] about = TestRandomUtil.nextBytes(156);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final byte[] phoneNumberSharing = TestRandomUtil.nextBytes(29);
    final String avatar = "profiles/" + ProfileTestHelper.generateRandomBase64FromByteArray(16);

    final Optional<VersionedProfileV1> profile = Optional.of(new VersionedProfileV1(HexFormat.of().formatHex(requestVersion), name, avatar, emoji, about, paymentAddress, phoneNumberSharing, new byte[0]));

    when(account.getCurrentProfileVersion()).thenReturn(Optional.ofNullable(accountVersion));
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));

    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.of(account));
    when(profilesManager.getV1(any(), any())).thenReturn(profile);

    final GetVersionedProfileAnonymousRequest request = GetVersionedProfileAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetVersionedProfileRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
                .build())
            .setVersion(ByteString.copyFrom(requestVersion))
            .build())
        .build();

    final GetVersionedProfileAnonymousResponse response = unauthenticatedServiceStub().getVersionedProfile(request);

    final GetVersionedProfileResult.Builder expectedResultBuilder = GetVersionedProfileResult.newBuilder()
        .setV1Response(GetVersionedProfileV1Response.newBuilder()
            .setName(ByteString.copyFrom(name))
            .setAbout(ByteString.copyFrom(about))
            .setAboutEmoji(ByteString.copyFrom(emoji))
            .setAvatar(avatar)
            .setPhoneNumberSharing(ByteString.copyFrom(phoneNumberSharing))
        );

    if (expectResponseHasPaymentAddress) {
      expectedResultBuilder.setPaymentAddressDataEtag(DataEtag.newBuilder()
          .setData(ByteString.copyFrom(paymentAddress))
          .setEtagSha256(ByteString.copyFrom(ProfileGrpcHelper.hash(paymentAddress)))
          .build());
    }

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
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);
    final byte[] data = TestRandomUtil.nextBytes(128);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final byte[] commitment = TestRandomUtil.nextBytes(97);
    final byte[] version = TestRandomUtil.nextBytes(32);
    final VersionedProfile v2Profile = new VersionedProfile(version, data, paymentAddress, commitment);

    when(account.getCurrentProfileVersion()).thenReturn(Optional.of(version));
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(account.hasCapability(DeviceCapability.PROFILES_V2)).thenReturn(true);
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.of(account));
    when(profilesManager.get(any(), any())).thenReturn(Optional.of(v2Profile));
    when(profilesManager.getV1(any(), any())).thenReturn(Optional.empty());

    final GetVersionedProfileAnonymousRequest request = GetVersionedProfileAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetVersionedProfileRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
                .build())
            .setVersion(ByteString.copyFrom(version))
            .build())
        .build();

    final GetVersionedProfileAnonymousResponse response = unauthenticatedServiceStub().getVersionedProfile(request);
    assertTrue(response.hasResult());

    final GetVersionedProfileResult result = response.getResult();
    assertTrue(result.hasDataEtag());
    assertEquals(ByteString.copyFrom(data), result.getDataEtag().getData());
    assertEquals(ByteString.copyFrom(v2Profile.dataHash()), result.getDataEtag().getEtagSha256());
    assertTrue(result.hasPaymentAddressDataEtag());
    assertEquals(ByteString.copyFrom(paymentAddress), result.getPaymentAddressDataEtag().getData());
    assertEquals(ByteString.copyFrom(Objects.requireNonNull(v2Profile.paymentAddressHash())), result.getPaymentAddressDataEtag().getEtagSha256());
    assertFalse(result.getDataEtagMatched());
    assertFalse(result.getPaymentAddressEtagMatched());
    assertFalse(result.hasV1Response());
  }

  @Test
  void getVersionedProfileV2EtagMatched() {
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);
    final byte[] data = TestRandomUtil.nextBytes(128);
    final byte[] paymentAddress = TestRandomUtil.nextBytes(582);
    final byte[] commitment = TestRandomUtil.nextBytes(97);
    final byte[] version = TestRandomUtil.nextBytes(32);
    final VersionedProfile v2Profile = new VersionedProfile(version, data, paymentAddress, commitment);

    when(account.getCurrentProfileVersion()).thenReturn(Optional.of(version));
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(account.hasCapability(DeviceCapability.PROFILES_V2)).thenReturn(true);
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.of(account));
    when(profilesManager.get(any(), any())).thenReturn(Optional.of(v2Profile));
    when(profilesManager.getV1(any(), any())).thenReturn(Optional.empty());

    final GetVersionedProfileRequest.Builder builder = GetVersionedProfileRequest.newBuilder()
        .setAccountIdentifier(ServiceIdentifier.newBuilder()
            .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
            .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
            .build())
        .setVersion(ByteString.copyFrom(version))
        .setDataEtag(ByteString.copyFrom(v2Profile.dataHash()));

    if (v2Profile.paymentAddressHash() != null) {
      builder.setPaymentAddressEtag(ByteString.copyFrom(v2Profile.paymentAddressHash()));
    }

    final GetVersionedProfileAnonymousRequest request = GetVersionedProfileAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(builder.build())
        .build();

    final GetVersionedProfileAnonymousResponse response = unauthenticatedServiceStub().getVersionedProfile(request);
    assertTrue(response.hasResult());

    final GetVersionedProfileResult result = response.getResult();
    assertTrue(result.getDataEtagMatched());
    assertTrue(result.getPaymentAddressEtagMatched());
    assertFalse(result.hasDataEtag());
    assertFalse(result.hasPaymentAddressDataEtag());
    assertFalse(result.hasV1Response());
  }

  @Test
  void getVersionedProfileVersionNotFound() {
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);

    when(accountsManager.getByServiceIdentifier(any())).thenReturn(Optional.of(account));
    when(profilesManager.getV1(any(), any())).thenReturn(Optional.empty());

    final GetVersionedProfileAnonymousRequest request = GetVersionedProfileAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetVersionedProfileRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
                .build())
            .setVersion(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .build())
        .build();

    final GetVersionedProfileAnonymousResponse response = unauthenticatedServiceStub().getVersionedProfile(
        request);

    assertTrue(response.hasNotFound());
  }

  @ParameterizedTest
  @MethodSource
  void getVersionedProfileUnauthenticated(final boolean missingUnidentifiedAccessKey, final boolean accountNotFound) {
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(accountsManager.getByServiceIdentifier(any())).thenReturn(
        accountNotFound ? Optional.empty() : Optional.of(account));

    final GetVersionedProfileAnonymousRequest.Builder requestBuilder = GetVersionedProfileAnonymousRequest.newBuilder()
        .setRequest(GetVersionedProfileRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
                .build())
            .setVersion(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .build());


    if (missingUnidentifiedAccessKey) {
      assertStatusException(Status.INVALID_ARGUMENT,
          () -> unauthenticatedServiceStub().getVersionedProfile(requestBuilder.build()));
    } else {
      requestBuilder.setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey));
      final GetVersionedProfileAnonymousResponse response = unauthenticatedServiceStub().getVersionedProfile(
          requestBuilder.build());

      assertEquals(accountNotFound, response.hasNotFound());
      assertNotEquals(accountNotFound, response.hasFailedUnidentifiedAuthorization());
    }


  }
  private static Stream<Arguments> getVersionedProfileUnauthenticated() {
    return Stream.of(
        Arguments.of(true, false),
        Arguments.of(false, true)
    );
  }

  @Test
  void getVersionedProfilePniInvalidArgument() {
    final byte[] unidentifiedAccessKey = TestRandomUtil.nextBytes(UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH);

    final GetVersionedProfileAnonymousRequest request = GetVersionedProfileAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetVersionedProfileRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_PNI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
                .build())
            .setVersion(ByteString.copyFrom(TestRandomUtil.nextBytes(32)))
            .build())
        .build();

    assertStatusException(Status.INVALID_ARGUMENT, () -> unauthenticatedServiceStub().getVersionedProfile(request));
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

  @Override
  protected List<ServerInterceptor> customizeInterceptors(List<ServerInterceptor> serverInterceptors) {
    return serverInterceptors.stream()
        // For now, don't validate error conformance because the profiles gRPC service has not been converted to the
        // updated error model
        .filter(interceptor -> !(interceptor instanceof ErrorConformanceInterceptor))
        .toList();
  }
}
