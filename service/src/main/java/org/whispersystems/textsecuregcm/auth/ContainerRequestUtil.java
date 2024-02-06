/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import org.glassfish.jersey.server.ContainerRequest;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import javax.ws.rs.core.SecurityContext;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

class ContainerRequestUtil {

  private static Map<Byte, Boolean> buildDevicesEnabledMap(final Account account) {
    return account.getDevices().stream().collect(Collectors.toMap(Device::getId, Device::isEnabled));
  }

  /**
   * A read-only subset of the authenticated Account object, to enforce that filter-based consumers do not perform
   * account modifying operations.
   */
  record AccountInfo(UUID accountId, String e164, Map<Byte, Boolean> devicesEnabled) {

    static AccountInfo fromAccount(final Account account) {
      return new AccountInfo(
          account.getUuid(),
          account.getNumber(),
          buildDevicesEnabledMap(account));
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
