/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

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
 */
public class AuthEnablementRefreshRequirementProvider implements WebsocketRefreshRequirementProvider {

  private final AccountsManager accountsManager;

  private static final Logger logger = LoggerFactory.getLogger(AuthEnablementRefreshRequirementProvider.class);

  private static final String ACCOUNT_UUID = AuthEnablementRefreshRequirementProvider.class.getName() + ".accountUuid";
  private static final String DEVICES_ENABLED = AuthEnablementRefreshRequirementProvider.class.getName() + ".devicesEnabled";

  public AuthEnablementRefreshRequirementProvider(final AccountsManager accountsManager) {
    this.accountsManager = accountsManager;
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
    setAccount(containerRequest, ContainerRequestUtil.AccountInfo.fromAccount(account));
  }

  private static void setAccount(final ContainerRequest containerRequest, final ContainerRequestUtil.AccountInfo info) {
    containerRequest.setProperty(ACCOUNT_UUID, info.accountId());
    containerRequest.setProperty(DEVICES_ENABLED, info.devicesEnabled());
  }

  @Override
  public List<Pair<UUID, Byte>> handleRequestFinished(final RequestEvent requestEvent) {
    // Now that the request is finished, check whether `isEnabled` changed for any of the devices. If the value did
    // change or if a devices was added or removed, all devices must disconnect and reauthenticate.
    if (requestEvent.getContainerRequest().getProperty(DEVICES_ENABLED) != null) {

      @SuppressWarnings("unchecked") final Map<Byte, Boolean> initialDevicesEnabled =
          (Map<Byte, Boolean>) requestEvent.getContainerRequest().getProperty(DEVICES_ENABLED);

      return accountsManager.getByAccountIdentifier((UUID) requestEvent.getContainerRequest().getProperty(ACCOUNT_UUID))
          .map(ContainerRequestUtil.AccountInfo::fromAccount)
          .map(account -> {
            final Set<Byte> deviceIdsToDisplace;
            final Map<Byte, Boolean> currentDevicesEnabled = account.devicesEnabled();

            if (!initialDevicesEnabled.equals(currentDevicesEnabled)) {
              deviceIdsToDisplace = new HashSet<>(initialDevicesEnabled.keySet());
              deviceIdsToDisplace.addAll(currentDevicesEnabled.keySet());
            } else {
              deviceIdsToDisplace = Collections.emptySet();
            }

            return deviceIdsToDisplace.stream()
                .map(deviceId -> new Pair<>(account.accountId(), deviceId))
                .collect(Collectors.toList());
          }).orElseGet(() -> {
            logger.error("Request had account, but it is no longer present");
            return Collections.emptyList();
          });
    } else {
      return Collections.emptyList();
    }
  }
}
