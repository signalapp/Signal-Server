package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Status;
import org.signal.chat.profile.GetUnversionedProfileAnonymousRequest;
import org.signal.chat.profile.GetUnversionedProfileResponse;
import org.signal.chat.profile.ReactorProfileAnonymousGrpc;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.badges.ProfileBadgeConverter;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import reactor.core.publisher.Mono;

public class ProfileAnonymousGrpcService extends ReactorProfileAnonymousGrpc.ProfileAnonymousImplBase {
  private final AccountsManager accountsManager;
  private final ProfileBadgeConverter profileBadgeConverter;

  public ProfileAnonymousGrpcService(
      final AccountsManager accountsManager,
      final ProfileBadgeConverter profileBadgeConverter) {
    this.accountsManager = accountsManager;
    this.profileBadgeConverter = profileBadgeConverter;
  }

  @Override
  public Mono<GetUnversionedProfileResponse> getUnversionedProfile(final GetUnversionedProfileAnonymousRequest request) {
    final ServiceIdentifier targetIdentifier =
        ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getRequest().getServiceIdentifier());

    // Callers must be authenticated to request unversioned profiles by PNI
    if (targetIdentifier.identityType() == IdentityType.PNI) {
      throw Status.UNAUTHENTICATED.asRuntimeException();
    }

    return getTargetAccountAndValidateUnidentifiedAccess(targetIdentifier, request.getUnidentifiedAccessKey().toByteArray())
        .map(targetAccount -> ProfileGrpcHelper.buildUnversionedProfileResponse(targetIdentifier,
            null,
            targetAccount,
            profileBadgeConverter));
  }

  private Mono<Account> getTargetAccountAndValidateUnidentifiedAccess(final ServiceIdentifier targetIdentifier, final byte[] unidentifiedAccessKey) {
    return Mono.fromFuture(() -> accountsManager.getByServiceIdentifierAsync(targetIdentifier))
        .flatMap(Mono::justOrEmpty)
        .filter(targetAccount -> UnidentifiedAccessUtil.checkUnidentifiedAccess(targetAccount, unidentifiedAccessKey))
        .switchIfEmpty(Mono.error(Status.UNAUTHENTICATED.asException()));
  }
}
