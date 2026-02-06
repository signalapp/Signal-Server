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


  public boolean checkGroupSendToken(final ByteString groupSendToken, final ServiceIdentifier serviceIdentifier) {
    return checkGroupSendToken(groupSendToken, List.of(serviceIdentifier.toLibsignal()));
  }

  public boolean checkGroupSendToken(final ByteString groupSendToken, final Collection<ServiceId> serviceIds) {
    try {
      final GroupSendFullToken token = new GroupSendFullToken(groupSendToken.toByteArray());
      final GroupSendDerivedKeyPair groupSendKeyPair =
          GroupSendDerivedKeyPair.forExpiration(token.getExpiration(), serverSecretParams);
      token.verify(serviceIds, clock.instant(), groupSendKeyPair);
      return true;
    } catch (final InvalidInputException e) {
      throw GrpcExceptions.fieldViolation("group_send_token", "malformed group send token");
    } catch (VerificationFailedException e) {
      return false;
    }
  }
}
