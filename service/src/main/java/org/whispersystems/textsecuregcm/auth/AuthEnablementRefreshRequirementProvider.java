/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Pair;

/**
 * This {@link WebsocketRefreshRequirementProvider} observes intra-request changes in {@link Account#isEnabled()} and
 * {@link Device#isEnabled()}.
 * <p>
 * If a change in {@link Account#isEnabled()} or any associated {@link Device#isEnabled()} is observed, then any active
 * WebSocket connections for the account must be closed in order for clients to get a refreshed
 * {@link io.dropwizard.auth.Auth} object with a current device list.
 *
 * @see AuthenticatedAccount
 * @see DisabledPermittedAuthenticatedAccount
 */
public class AuthEnablementRefreshRequirementProvider implements WebsocketRefreshRequirementProvider {

  private final AccountsManager accountsManager;

  private static final Logger logger = LoggerFactory.getLogger(AuthEnablementRefreshRequirementProvider.class);

  private static final String ACCOUNT_UUID = AuthEnablementRefreshRequirementProvider.class.getName() + ".accountUuid";
  private static final String DEVICES_ENABLED = AuthEnablementRefreshRequirementProvider.class.getName() + ".devicesEnabled";

  public AuthEnablementRefreshRequirementProvider(final AccountsManager accountsManager) {
    this.accountsManager = accountsManager;
  }

  @VisibleForTesting
  static Map<Long, Boolean> buildDevicesEnabledMap(final Account account) {
    return account.getDevices().stream().collect(Collectors.toMap(Device::getId, Device::isEnabled));
  }

  @Override
  public void handleRequestFiltered(final RequestEvent requestEvent) {
    if (requestEvent.getUriInfo().getMatchedResourceMethod().getInvocable().getHandlingMethod().getAnnotation(ChangesDeviceEnabledState.class) != null) {
      // The authenticated principal, if any, will be available after filters have run.
      // Now that the account is known, capture a snapshot of `isEnabled` for the account's devices before carrying out
      // the request’s business logic.
      ContainerRequestUtil.getAuthenticatedAccount(requestEvent.getContainerRequest()).ifPresent(account ->
          setAccount(requestEvent.getContainerRequest(), account));
    }
  }

  public static void setAccount(final ContainerRequest containerRequest, final Account account) {
    containerRequest.setProperty(ACCOUNT_UUID, account.getUuid());
    containerRequest.setProperty(DEVICES_ENABLED, buildDevicesEnabledMap(account));
  }

  @Override
  public List<Pair<UUID, Long>> handleRequestFinished(final RequestEvent requestEvent) {
    // Now that the request is finished, check whether `isEnabled` changed for any of the devices. If the value did
    // change or if a devices was added or removed, all devices must disconnect and reauthenticate.
    if (requestEvent.getContainerRequest().getProperty(DEVICES_ENABLED) != null) {

      @SuppressWarnings("unchecked") final Map<Long, Boolean> initialDevicesEnabled =
          (Map<Long, Boolean>) requestEvent.getContainerRequest().getProperty(DEVICES_ENABLED);

      return accountsManager.getByAccountIdentifier((UUID) requestEvent.getContainerRequest().getProperty(ACCOUNT_UUID)).map(account -> {
        final Set<Long> deviceIdsToDisplace;
        final Map<Long, Boolean> currentDevicesEnabled = buildDevicesEnabledMap(account);

        if (!initialDevicesEnabled.equals(currentDevicesEnabled)) {
          deviceIdsToDisplace = new HashSet<>(initialDevicesEnabled.keySet());
          deviceIdsToDisplace.addAll(currentDevicesEnabled.keySet());
        } else {
          deviceIdsToDisplace = Collections.emptySet();
        }

        return deviceIdsToDisplace.stream()
            .map(deviceId -> new Pair<>(account.getUuid(), deviceId))
            .collect(Collectors.toList());
      }).orElseGet(() -> {
        logger.error("Request had account, but it is no longer present");
        return Collections.emptyList();
      });
    } else
      return Collections.emptyList();
  }
}
