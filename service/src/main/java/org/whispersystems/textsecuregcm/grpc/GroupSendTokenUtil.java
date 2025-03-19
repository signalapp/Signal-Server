/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.protobuf.ByteString;
import io.grpc.Status;
import io.grpc.StatusException;
import java.time.Clock;
import java.util.Collection;
import java.util.List;
import org.signal.libsignal.protocol.ServiceId;
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.ServerSecretParams;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.groupsend.GroupSendDerivedKeyPair;
import org.signal.libsignal.zkgroup.groupsend.GroupSendFullToken;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;

public class GroupSendTokenUtil {

  private final ServerSecretParams serverSecretParams;
  private final Clock clock;

  public GroupSendTokenUtil(final ServerSecretParams serverSecretParams, final Clock clock) {
    this.serverSecretParams = serverSecretParams;
    this.clock = clock;
  }

  public void checkGroupSendToken(final ByteString serializedGroupSendToken,
      final ServiceIdentifier serviceIdentifier) throws StatusException {

    checkGroupSendToken(serializedGroupSendToken, List.of(serviceIdentifier.toLibsignal()));
  }

  public void checkGroupSendToken(final ByteString serializedGroupSendToken,
      final Collection<ServiceId> serviceIds) throws StatusException {

    try {
      final GroupSendFullToken token = new GroupSendFullToken(serializedGroupSendToken.toByteArray());
      token.verify(serviceIds, clock.instant(), GroupSendDerivedKeyPair.forExpiration(token.getExpiration(), serverSecretParams));
    } catch (final InvalidInputException e) {
      throw Status.INVALID_ARGUMENT.asException();
    } catch (VerificationFailedException e) {
      throw Status.UNAUTHENTICATED.asException();
    }
  }
}
