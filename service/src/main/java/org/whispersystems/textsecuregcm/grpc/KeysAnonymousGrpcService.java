/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Status;
import org.signal.chat.keys.GetPreKeysAnonymousRequest;
import org.signal.chat.keys.GetPreKeysResponse;
import org.signal.chat.keys.ReactorKeysAnonymousGrpc;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import reactor.core.publisher.Mono;

public class KeysAnonymousGrpcService extends ReactorKeysAnonymousGrpc.KeysAnonymousImplBase {

  private final AccountsManager accountsManager;
  private final KeysManager keysManager;

  public KeysAnonymousGrpcService(final AccountsManager accountsManager, final KeysManager keysManager) {
    this.accountsManager = accountsManager;
    this.keysManager = keysManager;
  }

  @Override
  public Mono<GetPreKeysResponse> getPreKeys(final GetPreKeysAnonymousRequest request) {
    final ServiceIdentifier serviceIdentifier =
        ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getRequest().getTargetIdentifier());

    return Mono.fromFuture(() -> accountsManager.getByServiceIdentifierAsync(serviceIdentifier))
        .flatMap(Mono::justOrEmpty)
        .switchIfEmpty(Mono.error(Status.UNAUTHENTICATED.asException()))
        .flatMap(targetAccount ->
            UnidentifiedAccessUtil.checkUnidentifiedAccess(targetAccount, request.getUnidentifiedAccessKey().toByteArray())
                ? KeysGrpcHelper.getPreKeys(targetAccount, serviceIdentifier.identityType(), request.getRequest().getDeviceId(), keysManager)
                : Mono.error(Status.UNAUTHENTICATED.asException()));
  }
}
