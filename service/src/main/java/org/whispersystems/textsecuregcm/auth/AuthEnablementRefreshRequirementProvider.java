/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.ws.rs.core.SecurityContext;
import org.glassfish.jersey.server.ContainerRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Pair;

/**
 * This {@link WebsocketRefreshRequirementProvider} observes intra-request changes in {@link Account#isEnabled()} and
 * {@link Device#isEnabled()}.
 * <p>
 * If a change in {@link Account#isEnabled()} is observed, then any active WebSocket connections for the account must be
 * closed, in order for clients to get a refreshed {@link io.dropwizard.auth.Auth} object.
 * <p>
 * If a change in {@link Device#isEnabled()} is observed, including deletion of the {@link Device}, then any active
 * WebSocket connections for the device must be closed and re-authenticated.
 *
 * @see AuthenticatedAccount
 * @see DisabledPermittedAuthenticatedAccount
 */
public class AuthEnablementRefreshRequirementProvider implements WebsocketRefreshRequirementProvider {

  private static final Logger logger = LoggerFactory.getLogger(AuthEnablementRefreshRequirementProvider.class);

  private static final String ACCOUNT_ENABLED = AuthEnablementRefreshRequirementProvider.class.getName() + ".accountEnabled";
  private static final String DEVICES_ENABLED = AuthEnablementRefreshRequirementProvider.class.getName() + ".devicesEnabled";

  private Optional<Account> findAccount(final ContainerRequest containerRequest) {
    return Optional.ofNullable(containerRequest.getSecurityContext())
        .map(SecurityContext::getUserPrincipal)
        .map(principal -> {
          if (principal instanceof AccountAndAuthenticatedDeviceHolder) {
            return ((AccountAndAuthenticatedDeviceHolder) principal).getAccount();
          }
          return null;
        });
  }

  @VisibleForTesting
  Map<Long, Boolean> buildDevicesEnabledMap(final Account account) {
    return account.getDevices().stream()
        .collect(() -> new HashMap<>(account.getDevices().size()),
            (map, device) -> map.put(device.getId(), device.isEnabled()), HashMap::putAll);
  }

  @Override
  public void handleRequestStart(final ContainerRequest request) {
    // The authenticated principal, if any, will be available after filters have run.
    // Now that the account is known, capture a snapshot of `isEnabled` for the account and its devices,
    // before carrying out the request’s business logic.
    findAccount(request)
        .ifPresent(
            account -> {
              request.setProperty(ACCOUNT_ENABLED, account.isEnabled());
              request.setProperty(DEVICES_ENABLED, buildDevicesEnabledMap(account));
            });
  }

  @Override
  public List<Pair<UUID, Long>> handleRequestFinished(final ContainerRequest request) {
    // Now that the request is finished, check whether `isEnabled` changed for any of the devices, or the account
    // as a whole. If the value did change, the affected device(s) must disconnect and reauthenticate.
    // If a device was removed, it must also disconnect.
    if (request.getProperty(ACCOUNT_ENABLED) != null &&
        request.getProperty(DEVICES_ENABLED) != null) {

      final boolean accountInitiallyEnabled = (boolean) request.getProperty(ACCOUNT_ENABLED);
      @SuppressWarnings("unchecked") final Map<Long, Boolean> initialDevicesEnabled =
          (Map<Long, Boolean>) request.getProperty(DEVICES_ENABLED);

      return findAccount(request).map(account -> {
        final Set<Long> deviceIdsToDisplace;

        if (account.isEnabled() != accountInitiallyEnabled) {
          // the @Auth for all active connections must change when account.isEnabled() changes
          deviceIdsToDisplace = account.getDevices().stream()
              .map(Device::getId).collect(Collectors.toSet());

          deviceIdsToDisplace.addAll(initialDevicesEnabled.keySet());

        } else if (!initialDevicesEnabled.isEmpty()) {

          deviceIdsToDisplace = new HashSet<>();
          final Map<Long, Boolean> currentDevicesEnabled = buildDevicesEnabledMap(account);

          initialDevicesEnabled.forEach((deviceId, enabled) -> {
            // `null` indicates the device was removed from the account. Any active presence should be removed.
            final boolean enabledMatches = Objects.equals(enabled,
                currentDevicesEnabled.getOrDefault(deviceId, null));

            if (!enabledMatches) {
              deviceIdsToDisplace.add(deviceId);
            }
          });
        } else {
          deviceIdsToDisplace = Collections.emptySet();
        }

        return deviceIdsToDisplace.stream().map(deviceId -> new Pair<>(account.getUuid(), deviceId))
            .collect(Collectors.toList());
      }).orElseGet(() -> {
        logger.error("Request had account, but it is no longer present");
        return Collections.emptyList();
      });
    } else
      return Collections.emptyList();
  }
}
