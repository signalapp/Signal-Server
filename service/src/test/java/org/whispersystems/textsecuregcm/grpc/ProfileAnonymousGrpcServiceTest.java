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
import org.whispersystems.textsecuregcm.util.UUIDUtil;

public class ProfileAnonymousGrpcServiceTest {
  private Account account;
  private AccountsManager accountsManager;
  private ProfileBadgeConverter profileBadgeConverter;
  private ProfileAnonymousGrpc.ProfileAnonymousBlockingStub profileAnonymousBlockingStub;

  @RegisterExtension
  static final GrpcServerExtension GRPC_SERVER_EXTENSION = new GrpcServerExtension();

  @BeforeEach
  void setup() {
    account = mock(Account.class);
    accountsManager = mock(AccountsManager.class);
    profileBadgeConverter = mock(ProfileBadgeConverter.class);

    final Metadata metadata = new Metadata();
    metadata.put(AcceptLanguageInterceptor.ACCEPTABLE_LANGUAGES_GRPC_HEADER, "en-us");

    profileAnonymousBlockingStub = ProfileAnonymousGrpc.newBlockingStub(GRPC_SERVER_EXTENSION.getChannel())
        .withInterceptors(MetadataUtils.newAttachHeadersInterceptor(metadata));

    final ProfileAnonymousGrpcService profileAnonymousGrpcService = new ProfileAnonymousGrpcService(
        accountsManager,
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
}
