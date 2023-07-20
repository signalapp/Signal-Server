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
    return KeysGrpcHelper.findAccount(request.getTargetIdentifier(), accountsManager)
        .switchIfEmpty(Mono.error(Status.UNAUTHENTICATED.asException()))
        .flatMap(targetAccount -> {
          final IdentityType identityType =
              IdentityType.fromGrpcIdentityType(request.getTargetIdentifier().getIdentityType());

          return UnidentifiedAccessUtil.checkUnidentifiedAccess(targetAccount, request.getUnidentifiedAccessKey().toByteArray())
              ? KeysGrpcHelper.getPreKeys(targetAccount, identityType, request.getDeviceId(), keysManager)
              : Mono.error(Status.UNAUTHENTICATED.asException());
        });
  }
}
