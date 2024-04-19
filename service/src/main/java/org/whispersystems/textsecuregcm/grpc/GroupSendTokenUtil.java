/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;

import java.time.Clock;
import java.util.List;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.groupsend.GroupSendDerivedKeyPair;
import org.signal.libsignal.zkgroup.groupsend.GroupSendFullToken;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;

import reactor.core.publisher.Mono;

public class GroupSendTokenUtil {

  private final ServerSecretParams serverSecretParams;
  private final Clock clock;

  public GroupSendTokenUtil(final ServerSecretParams serverSecretParams, final Clock clock) {
    this.serverSecretParams = serverSecretParams;
    this.clock = clock;
  }

  public Mono<Void> checkGroupSendToken(final ByteString serializedGroupSendToken, List<ServiceIdentifier> serviceIdentifiers) {
    try {
      final GroupSendFullToken token = new GroupSendFullToken(serializedGroupSendToken.toByteArray());
      final List<ServiceId> serviceIds = serviceIdentifiers.stream().map(ServiceIdentifier::toLibsignal).toList();
      token.verify(serviceIds, clock.instant(), GroupSendDerivedKeyPair.forExpiration(token.getExpiration(), serverSecretParams));
      return Mono.empty();
    } catch (InvalidInputException e) {
      return Mono.error(Status.INVALID_ARGUMENT.asException());
    } catch (VerificationFailedException e) {
      return Mono.error(Status.UNAUTHENTICATED.asException());
    }
  }
}
