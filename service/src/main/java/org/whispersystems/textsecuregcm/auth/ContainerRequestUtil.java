/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import jakarta.ws.rs.core.SecurityContext;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.glassfish.jersey.server.ContainerRequest;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

class ContainerRequestUtil {

  /**
   * A read-only subset of the authenticated Account object, to enforce that filter-based consumers do not perform
   * account modifying operations.
   */
  record AccountInfo(UUID accountId, String e164, Set<Byte> deviceIds) {

    static AccountInfo fromAccount(final Account account) {
      return new AccountInfo(
          account.getUuid(),
          account.getNumber(),
          account.getDevices().stream().map(Device::getId).collect(Collectors.toSet()));
    }
  }

  static Optional<AccountInfo> getAuthenticatedAccount(final ContainerRequest request) {
    return Optional.ofNullable(request.getSecurityContext())
        .map(SecurityContext::getUserPrincipal)
        .map(principal -> {
          if (principal instanceof AccountAndAuthenticatedDeviceHolder aaadh) {
            return aaadh.getAccount();
          }
          return null;
        })
        .map(AccountInfo::fromAccount);
  }
}
