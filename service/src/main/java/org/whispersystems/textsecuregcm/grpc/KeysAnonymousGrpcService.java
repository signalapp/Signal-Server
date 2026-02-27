/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Clock;
import java.util.Arrays;
import java.util.concurrent.Flow;
import org.signal.chat.errors.FailedUnidentifiedAuthorization;
import org.signal.chat.errors.NotFound;
import org.signal.chat.keys.CheckIdentityKeyRequest;
import org.signal.chat.keys.CheckIdentityKeyResponse;
import org.signal.chat.keys.GetPreKeysAnonymousRequest;
import org.signal.chat.keys.GetPreKeysAnonymousResponse;
import org.signal.chat.keys.SimpleKeysAnonymousGrpc;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.KeysManager;
import reactor.adapter.JdkFlowAdapter;
import reactor.core.publisher.Mono;
import reactor.util.function.Tuples;

public class KeysAnonymousGrpcService extends SimpleKeysAnonymousGrpc.KeysAnonymousImplBase {

  private final AccountsManager accountsManager;
  private final KeysManager keysManager;
  private final GroupSendTokenUtil groupSendTokenUtil;

  public KeysAnonymousGrpcService(
      final AccountsManager accountsManager, final KeysManager keysManager, final ServerSecretParams serverSecretParams, final Clock clock) {
    this.accountsManager = accountsManager;
    this.keysManager = keysManager;
    groupSendTokenUtil = new GroupSendTokenUtil(serverSecretParams, clock);
  }

  @Override
  public GetPreKeysAnonymousResponse getPreKeys(final GetPreKeysAnonymousRequest request) {
    final ServiceIdentifier serviceIdentifier =
        ServiceIdentifierUtil.fromGrpcServiceIdentifier(request.getRequest().getTargetIdentifier());

    final byte deviceId = request.getRequest().hasDeviceId()
        ? DeviceIdUtil.validate(request.getRequest().getDeviceId())
        : KeysGrpcHelper.ALL_DEVICES;

    return switch (request.getAuthorizationCase()) {
      case GROUP_SEND_TOKEN -> {
        if (!groupSendTokenUtil.checkGroupSendToken(request.getGroupSendToken(), serviceIdentifier)) {
          yield GetPreKeysAnonymousResponse.newBuilder()
              .setFailedUnidentifiedAuthorization(FailedUnidentifiedAuthorization.getDefaultInstance())
              .build();
        }

        yield accountsManager.getByServiceIdentifier(serviceIdentifier)
            .flatMap(targetAccount -> KeysGrpcHelper.getPreKeys(targetAccount, serviceIdentifier, deviceId, keysManager))
            .map(accountPreKeyBundles -> GetPreKeysAnonymousResponse.newBuilder().setPreKeys(accountPreKeyBundles).build())
            .orElseGet(() ->  GetPreKeysAnonymousResponse.newBuilder()
                .setTargetNotFound(NotFound.getDefaultInstance())
                .build());
      }

      case UNIDENTIFIED_ACCESS_KEY -> accountsManager.getByServiceIdentifier(serviceIdentifier)
          .filter(targetAccount -> UnidentifiedAccessUtil.checkUnidentifiedAccess(targetAccount, request.getUnidentifiedAccessKey().toByteArray()))
          .flatMap(targetAccount -> KeysGrpcHelper.getPreKeys(targetAccount, serviceIdentifier, deviceId, keysManager))
          .map(accountPreKeyBundles -> GetPreKeysAnonymousResponse.newBuilder().setPreKeys(accountPreKeyBundles).build())
          .orElseGet(() -> GetPreKeysAnonymousResponse.newBuilder()
              .setFailedUnidentifiedAuthorization(FailedUnidentifiedAuthorization.getDefaultInstance())
              .build());

      default -> throw GrpcExceptions.fieldViolation("authorization", "invalid authorization type");
    };
  }

  @Override
  public Flow.Publisher<CheckIdentityKeyResponse> checkIdentityKeys(final Flow.Publisher<CheckIdentityKeyRequest> requests) {
    return JdkFlowAdapter.publisherToFlowPublisher(JdkFlowAdapter.flowPublisherToFlux(requests)
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
                .build())));
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
