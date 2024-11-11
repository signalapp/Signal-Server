/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import jakarta.ws.rs.NotAuthorizedException;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.core.Response;
import java.security.MessageDigest;
import java.util.Optional;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class OptionalAccess {

  public static String ALL_DEVICES_SELECTOR = "*";

  public static void verify(Optional<Account> requestAccount,
      Optional<Anonymous> accessKey,
      Optional<Account> targetAccount,
      ServiceIdentifier targetIdentifier,
      String deviceSelector) {

    try {
      verify(requestAccount, accessKey, targetAccount, targetIdentifier);

      if (!ALL_DEVICES_SELECTOR.equals(deviceSelector)) {
        byte deviceId = Byte.parseByte(deviceSelector);

        Optional<Device> targetDevice = targetAccount.get().getDevice(deviceId);

        if (targetDevice.isPresent()) {
          return;
        }

        if (requestAccount.isPresent()) {
          throw new NotFoundException();
        } else {
          throw new NotAuthorizedException(Response.Status.UNAUTHORIZED);
        }
      }
    } catch (NumberFormatException e) {
      throw new WebApplicationException(Response.status(422).build());
    }
  }

  public static void verify(Optional<Account> requestAccount,
      Optional<Anonymous> accessKey,
      Optional<Account> targetAccount,
      ServiceIdentifier targetIdentifier) {

    if (requestAccount.isPresent()) {
      // Authenticated requests are never unauthorized; if the target exists, return OK, otherwise throw not-found.
      if (targetAccount.isPresent()) {
        return;
      } else {
        throw new NotFoundException();
      }
    }

    // Anything past this point can only be authenticated by an access key. Even when the target
    // has unrestricted unidentified access, callers need to supply a fake access key. Likewise, if
    // the target account does not exist, we *also* report unauthorized here (*not* not-found,
    // since that would provide a free exists check).
    if (accessKey.isEmpty() || targetAccount.isEmpty()) {
      throw new NotAuthorizedException(Response.Status.UNAUTHORIZED);
    }

    // Unrestricted unidentified access does what it says on the tin: we don't check if the key the
    // caller provided is right or not.
    if (targetAccount.get().isUnrestrictedUnidentifiedAccess()) {
      return;
    }

    if (!targetAccount.get().isIdentifiedBy(targetIdentifier)) {
      throw new IllegalArgumentException("Target account is not identified by the given identifier");
    }

    // Unidentified access is only for ACI identities
    if (IdentityType.PNI.equals(targetIdentifier.identityType())) {
      throw new NotAuthorizedException(Response.Status.UNAUTHORIZED);
    }

    // At this point, any successful authentication requires a real access key on the target account
    if (targetAccount.get().getUnidentifiedAccessKey().isEmpty()) {
      throw new NotAuthorizedException(Response.Status.UNAUTHORIZED);
    }

    // Otherwise, access is gated by the caller having the unidentified-access key matching the target account.
    if (MessageDigest.isEqual(accessKey.get().getAccessKey(), targetAccount.get().getUnidentifiedAccessKey().get())) {
      return;
    }

    throw new NotAuthorizedException(Response.Status.UNAUTHORIZED);
  }

}
