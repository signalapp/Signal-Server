package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import io.grpc.Metadata;
import io.grpc.Status;
import java.security.SecureRandom;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.MetadataUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.signal.chat.common.IdentityType;
import org.signal.chat.common.ServiceIdentifier;
import org.signal.chat.profile.GetUnversionedProfileAnonymousRequest;
import org.signal.chat.profile.GetUnversionedProfileRequest;
import org.signal.chat.profile.GetUnversionedProfileResponse;
import org.signal.chat.profile.GetVersionedProfileAnonymousRequest;
import org.signal.chat.profile.GetVersionedProfileRequest;
import org.signal.chat.profile.GetVersionedProfileResponse;
import org.signal.chat.profile.ProfileAnonymousGrpc;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.Curve;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessChecksum;
import org.whispersystems.textsecuregcm.badges.ProfileBadgeConverter;
import org.whispersystems.textsecuregcm.entities.Badge;
import org.whispersystems.textsecuregcm.entities.BadgeSvg;
import org.whispersystems.textsecuregcm.entities.UserCapabilities;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.ProfilesManager;
import org.whispersystems.textsecuregcm.storage.VersionedProfile;
import org.whispersystems.textsecuregcm.tests.util.ProfileHelper;
import org.whispersystems.textsecuregcm.util.UUIDUtil;
import javax.annotation.Nullable;

public class ProfileAnonymousGrpcServiceTest {
  private Account account;
  private AccountsManager accountsManager;
  private ProfilesManager profilesManager;
  private ProfileBadgeConverter profileBadgeConverter;
  private ProfileAnonymousGrpc.ProfileAnonymousBlockingStub profileAnonymousBlockingStub;

  @RegisterExtension
  static final GrpcServerExtension GRPC_SERVER_EXTENSION = new GrpcServerExtension();

  @BeforeEach
  void setup() {
    account = mock(Account.class);
    accountsManager = mock(AccountsManager.class);
    profilesManager = mock(ProfilesManager.class);
    profileBadgeConverter = mock(ProfileBadgeConverter.class);

    final Metadata metadata = new Metadata();
    metadata.put(AcceptLanguageInterceptor.ACCEPTABLE_LANGUAGES_GRPC_HEADER, "en-us");
    metadata.put(UserAgentInterceptor.USER_AGENT_GRPC_HEADER, "Signal-Android/1.2.3");

    profileAnonymousBlockingStub = ProfileAnonymousGrpc.newBlockingStub(GRPC_SERVER_EXTENSION.getChannel())
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

    final ProfileAnonymousGrpcService profileAnonymousGrpcService = new ProfileAnonymousGrpcService(
        accountsManager,
        profilesManager,
        profileBadgeConverter
    );

    GRPC_SERVER_EXTENSION.getServiceRegistry()
        .addService(profileAnonymousGrpcService);
  }

  @Test
  void getUnversionedProfile() {
    final UUID targetUuid = UUID.randomUUID();
    final org.whispersystems.textsecuregcm.identity.ServiceIdentifier serviceIdentifier = new AciServiceIdentifier(targetUuid);

    final byte[] unidentifiedAccessKey = new byte[16];
    new SecureRandom().nextBytes(unidentifiedAccessKey);
    final ECKeyPair identityKeyPair = Curve.generateKeyPair();
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
    when(accountsManager.getByServiceIdentifierAsync(serviceIdentifier)).thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

    final GetUnversionedProfileAnonymousRequest request = GetUnversionedProfileAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetUnversionedProfileRequest.newBuilder()
            .setServiceIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(targetUuid)))
                .build())
            .build())
        .build();

    final GetUnversionedProfileResponse response = profileAnonymousBlockingStub.getUnversionedProfile(request);

    final byte[] unidentifiedAccessChecksum = UnidentifiedAccessChecksum.generateFor(unidentifiedAccessKey);
    final GetUnversionedProfileResponse expectedResponse = GetUnversionedProfileResponse.newBuilder()
        .setIdentityKey(ByteString.copyFrom(identityKey.serialize()))
        .setUnidentifiedAccess(ByteString.copyFrom(unidentifiedAccessChecksum))
        .setUnrestrictedUnidentifiedAccess(false)
        .setCapabilities(ProfileGrpcHelper.buildUserCapabilities(UserCapabilities.createForAccount(account)))
        .addAllBadges(ProfileGrpcHelper.buildBadges(badges))
        .build();

    verify(accountsManager).getByServiceIdentifierAsync(serviceIdentifier);
    assertEquals(expectedResponse, response);
  }

  @ParameterizedTest
  @MethodSource
  void getUnversionedProfileUnauthenticated(final IdentityType identityType, final boolean missingUnidentifiedAccessKey, final boolean accountNotFound) {
    final byte[] unidentifiedAccessKey = new byte[16];
    new SecureRandom().nextBytes(unidentifiedAccessKey);

    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);
    when(accountsManager.getByServiceIdentifierAsync(any())).thenReturn(
        CompletableFuture.completedFuture(accountNotFound ? Optional.empty() : Optional.of(account)));

    final GetUnversionedProfileAnonymousRequest.Builder requestBuilder = GetUnversionedProfileAnonymousRequest.newBuilder()
        .setRequest(GetUnversionedProfileRequest.newBuilder()
            .setServiceIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(identityType)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
                .build())
            .build());

    if (!missingUnidentifiedAccessKey) {
      requestBuilder.setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey));
    }

    final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class,
        () -> profileAnonymousBlockingStub.getUnversionedProfile(requestBuilder.build()));

    assertEquals(Status.UNAUTHENTICATED.getCode(), statusRuntimeException.getStatus().getCode());
  }

  private static Stream<Arguments> getUnversionedProfileUnauthenticated() {
    return Stream.of(
        Arguments.of(IdentityType.IDENTITY_TYPE_PNI, false, false),
        Arguments.of(IdentityType.IDENTITY_TYPE_ACI, true, false),
        Arguments.of(IdentityType.IDENTITY_TYPE_ACI, false, true)
    );
  }

  @ParameterizedTest
  @MethodSource
  void getVersionedProfile(final String requestVersion,
      @Nullable final String accountVersion,
      final boolean expectResponseHasPaymentAddress) {
    final byte[] unidentifiedAccessKey = new byte[16];
    new SecureRandom().nextBytes(unidentifiedAccessKey);

    final VersionedProfile profile = mock(VersionedProfile.class);
    final byte[] name = ProfileHelper.generateRandomByteArray(81);
    final byte[] emoji = ProfileHelper.generateRandomByteArray(60);
    final byte[] about = ProfileHelper.generateRandomByteArray(156);
    final byte[] paymentAddress = ProfileHelper.generateRandomByteArray(582);
    final String avatar = "profiles/" + ProfileHelper.generateRandomBase64FromByteArray(16);

    when(profile.name()).thenReturn(name);
    when(profile.aboutEmoji()).thenReturn(emoji);
    when(profile.about()).thenReturn(about);
    when(profile.paymentAddress()).thenReturn(paymentAddress);
    when(profile.avatar()).thenReturn(avatar);

    when(account.getCurrentProfileVersion()).thenReturn(Optional.ofNullable(accountVersion));
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));

    when(accountsManager.getByServiceIdentifierAsync(any())).thenReturn(CompletableFuture.completedFuture(Optional.of(account)));
    when(profilesManager.getAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(Optional.of(profile)));

    final GetVersionedProfileAnonymousRequest request = GetVersionedProfileAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetVersionedProfileRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
                .build())
            .setVersion(requestVersion)
            .build())
        .build();

    final GetVersionedProfileResponse response = profileAnonymousBlockingStub.getVersionedProfile(request);

    final GetVersionedProfileResponse.Builder expectedResponseBuilder = GetVersionedProfileResponse.newBuilder()
        .setName(ByteString.copyFrom(name))
        .setAbout(ByteString.copyFrom(about))
        .setAboutEmoji(ByteString.copyFrom(emoji))
        .setAvatar(avatar);

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

  @Test
  void getVersionedProfileVersionNotFound() {
    final byte[] unidentifiedAccessKey = new byte[16];
    new SecureRandom().nextBytes(unidentifiedAccessKey);

    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);

    when(accountsManager.getByServiceIdentifierAsync(any())).thenReturn(CompletableFuture.completedFuture(Optional.of(account)));
    when(profilesManager.getAsync(any(), any())).thenReturn(CompletableFuture.completedFuture(Optional.empty()));

    final GetVersionedProfileAnonymousRequest request = GetVersionedProfileAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetVersionedProfileRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
                .build())
            .setVersion("someVersion")
            .build())
        .build();

    final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class,
        () -> profileAnonymousBlockingStub.getVersionedProfile(request));

    assertEquals(Status.NOT_FOUND.getCode(), statusRuntimeException.getStatus().getCode());
  }

  @ParameterizedTest
  @MethodSource
  void getVersionedProfileUnauthenticated(final boolean missingUnidentifiedAccessKey,
      final boolean accountNotFound) {
    final byte[] unidentifiedAccessKey = new byte[16];
    new SecureRandom().nextBytes(unidentifiedAccessKey);

    when(account.isUnrestrictedUnidentifiedAccess()).thenReturn(false);
    when(account.getUnidentifiedAccessKey()).thenReturn(Optional.of(unidentifiedAccessKey));
    when(accountsManager.getByServiceIdentifierAsync(any())).thenReturn(
        CompletableFuture.completedFuture(accountNotFound ? Optional.empty() : Optional.of(account)));

    final GetVersionedProfileAnonymousRequest.Builder requestBuilder = GetVersionedProfileAnonymousRequest.newBuilder()
        .setRequest(GetVersionedProfileRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_ACI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
                .build())
            .setVersion("someVersion")
            .build());

    if (!missingUnidentifiedAccessKey) {
      requestBuilder.setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey));
    }

    final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class,
        () -> profileAnonymousBlockingStub.getVersionedProfile(requestBuilder.build()));

    assertEquals(Status.UNAUTHENTICATED.getCode(), statusRuntimeException.getStatus().getCode());
  }

  private static Stream<Arguments> getVersionedProfileUnauthenticated() {
    return Stream.of(
        Arguments.of(true, false),
        Arguments.of(false, true)
    );
  }
  @Test
  void getVersionedProfilePniInvalidArgument() {
    final byte[] unidentifiedAccessKey = new byte[16];
    new SecureRandom().nextBytes(unidentifiedAccessKey);

    final GetVersionedProfileAnonymousRequest request = GetVersionedProfileAnonymousRequest.newBuilder()
        .setUnidentifiedAccessKey(ByteString.copyFrom(unidentifiedAccessKey))
        .setRequest(GetVersionedProfileRequest.newBuilder()
            .setAccountIdentifier(ServiceIdentifier.newBuilder()
                .setIdentityType(IdentityType.IDENTITY_TYPE_PNI)
                .setUuid(ByteString.copyFrom(UUIDUtil.toBytes(UUID.randomUUID())))
                .build())
            .setVersion("someVersion")
            .build())
        .build();

    final StatusRuntimeException statusRuntimeException = assertThrows(StatusRuntimeException.class,
        () -> profileAnonymousBlockingStub.getVersionedProfile(request));

    assertEquals(Status.INVALID_ARGUMENT.getCode(), statusRuntimeException.getStatus().getCode());
  }
}
