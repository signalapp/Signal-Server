/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.Arrays;
import org.signal.chat.keys.CheckIdentityKeyRequest;
import org.signal.chat.keys.CheckIdentityKeyResponse;
import org.signal.chat.keys.GetPreKeysAnonymousRequest;
import org.signal.chat.keys.GetPreKeysResponse;
import org.signal.chat.keys.ReactorKeysAnonymousGrpc;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

public class KeysAnonymousGrpcService extends ReactorKeysAnonymousGrpc.KeysAnonymousImplBase {

  private final AccountsManager accountsManager;
  private final KeysManager keysManager;
  private final GroupSendTokenUtil groupSendTokenUtil;

  public KeysAnonymousGrpcService(
      final AccountsManager accountsManager, final KeysManager keysManager, final ServerSecretParams serverSecretParams, final Clock clock) {
    this.accountsManager = accountsManager;
    this.keysManager = keysManager;
    this.groupSendTokenUtil = new GroupSendTokenUtil(serverSecretParams, clock);
}

  @Override
  public Mono<GetPreKeysResponse> getPreKeys(final GetPreKeysAnonymousRequest request) {
    final ServiceIdentifier serviceIdentifier =
        ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getRequest().getTargetIdentifier());

    final byte deviceId = request.getRequest().hasDeviceId()
        ? DeviceIdUtil.validate(request.getRequest().getDeviceId())
        : KeysGrpcHelper.ALL_DEVICES;

    return switch (request.getAuthorizationCase()) {
      case GROUP_SEND_TOKEN -> {
        try {
          groupSendTokenUtil.checkGroupSendToken(request.getGroupSendToken(), serviceIdentifier);

          yield lookUpAccount(serviceIdentifier, Status.NOT_FOUND)
              .flatMap(targetAccount -> KeysGrpcHelper.getPreKeys(targetAccount, serviceIdentifier.identityType(), deviceId, keysManager));
        } catch (final StatusException e) {
          yield Mono.error(e);
        }
      }

      case UNIDENTIFIED_ACCESS_KEY ->
          lookUpAccount(serviceIdentifier, Status.UNAUTHENTICATED)
              .flatMap(targetAccount ->
                  UnidentifiedAccessUtil.checkUnidentifiedAccess(targetAccount, request.getUnidentifiedAccessKey().toByteArray())
                  ? KeysGrpcHelper.getPreKeys(targetAccount, serviceIdentifier.identityType(), deviceId, keysManager)
                  : Mono.error(Status.UNAUTHENTICATED.asException()));

      default -> Mono.error(Status.INVALID_ARGUMENT.asException());
    };
  }

  @Override
  public Flux<CheckIdentityKeyResponse> checkIdentityKeys(final Flux<CheckIdentityKeyRequest> requests) {
    return requests
        .map(request -> Tuples.of(ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getTargetIdentifier()),
            request.getFingerprint().toByteArray()))
        .flatMap(serviceIdentifierAndFingerprint -> Mono.fromFuture(
                () -> accountsManager.getByServiceIdentifierAsync(serviceIdentifierAndFingerprint.getT1()))
            .flatMap(Mono::justOrEmpty)
            .filter(account -> !fingerprintMatches(account.getIdentityKey(serviceIdentifierAndFingerprint.getT1()
                .identityType()), serviceIdentifierAndFingerprint.getT2()))
            .map(account -> CheckIdentityKeyResponse.newBuilder()
                    .setTargetIdentifier(
                        ServiceIdentifierUtil.toGrpcServiceIdentifier(serviceIdentifierAndFingerprint.getT1()))
                    .setIdentityKey(ByteString.copyFrom(account.getIdentityKey(serviceIdentifierAndFingerprint.getT1()
                        .identityType()).serialize()))
                    .build())
        );
  }

  private Mono<Account> lookUpAccount(final ServiceIdentifier serviceIdentifier, final Status onNotFound) {
    return Mono.fromFuture(() -> accountsManager.getByServiceIdentifierAsync(serviceIdentifier))
      .flatMap(Mono::justOrEmpty)
      .switchIfEmpty(Mono.error(onNotFound.asException()));
  }

  private static boolean fingerprintMatches(final IdentityKey identityKey, final byte[] fingerprint) {
    final byte[] digest;
    try {
      digest = MessageDigest.getInstance("SHA-256").digest(identityKey.serialize());
    } catch (NoSuchAlgorithmException e) {
      // SHA-256 should always be supported as an algorithm
      throw new AssertionError("All Java implementations must support the SHA-256 message digest");
    }

    return Arrays.equals(digest, 0, 4, fingerprint, 0, 4);
  }
}
