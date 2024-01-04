/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

import javax.ws.rs.NotAuthorizedException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import java.security.MessageDigest;
import java.util.Optional;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class OptionalAccess {

  public static void verify(Optional<Account>   requestAccount,
                            Optional<Anonymous> accessKey,
                            Optional<Account>   targetAccount,
                            String              deviceSelector)
  {
    try {
      verify(requestAccount, accessKey, targetAccount);

      if (!deviceSelector.equals("*")) {
        byte deviceId = Byte.parseByte(deviceSelector);

        Optional<Device> targetDevice = targetAccount.get().getDevice(deviceId);

        if (targetDevice.isPresent() && targetDevice.get().isEnabled()) {
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

  public static void verify(Optional<Account>   requestAccount,
                            Optional<Anonymous> accessKey,
                            Optional<Account>   targetAccount)
  {
    if (requestAccount.isPresent() && targetAccount.isPresent() && targetAccount.get().isEnabled()) {
      return;
    }

    //noinspection ConstantConditions
    if (requestAccount.isPresent() && (targetAccount.isEmpty() || (targetAccount.isPresent() && !targetAccount.get().isEnabled()))) {
      throw new NotFoundException();
    }

    if (accessKey.isPresent() && targetAccount.isPresent() && targetAccount.get().isEnabled() && targetAccount.get().isUnrestrictedUnidentifiedAccess()) {
      return;
    }

    if (accessKey.isPresent()                                      &&
        targetAccount.isPresent()                                  &&
        targetAccount.get().getUnidentifiedAccessKey().isPresent() &&
        targetAccount.get().isEnabled()                            &&
        MessageDigest.isEqual(accessKey.get().getAccessKey(), targetAccount.get().getUnidentifiedAccessKey().get()))
    {
      return;
    }

    throw new NotAuthorizedException(Response.Status.UNAUTHORIZED);
  }

}
