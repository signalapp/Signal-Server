/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.Pair;

/**
 * This {@link WebsocketRefreshRequirementProvider} observes intra-request changes in devices linked to an
 * {@link Account} and triggers a WebSocket refresh if that set changes. If a change in linked devices is observed, then
 * any active WebSocket connections for the account must be closed in order for clients to get a refreshed
 * {@link io.dropwizard.auth.Auth} object with a current device list.
 *
 * @see AuthenticatedDevice
 */
public class LinkedDeviceRefreshRequirementProvider implements WebsocketRefreshRequirementProvider {

  private final AccountsManager accountsManager;

  private static final Logger logger = LoggerFactory.getLogger(LinkedDeviceRefreshRequirementProvider.class);

  private static final String ACCOUNT_UUID = LinkedDeviceRefreshRequirementProvider.class.getName() + ".accountUuid";
  private static final String LINKED_DEVICE_IDS = LinkedDeviceRefreshRequirementProvider.class.getName() + ".deviceIds";

  public LinkedDeviceRefreshRequirementProvider(final AccountsManager accountsManager) {
    this.accountsManager = accountsManager;
  }

  @Override
  public void handleRequestFiltered(final RequestEvent requestEvent) {
    if (requestEvent.getUriInfo().getMatchedResourceMethod().getInvocable().getHandlingMethod().getAnnotation(
        ChangesLinkedDevices.class) != null) {
      // The authenticated principal, if any, will be available after filters have run. Now that the account is known,
      // capture a snapshot of the account's linked devices before carrying out the request’s business logic.
      ContainerRequestUtil.getAuthenticatedAccount(requestEvent.getContainerRequest())
          .ifPresent(account -> setAccount(requestEvent.getContainerRequest(), account));
    }
  }

  public static void setAccount(final ContainerRequest containerRequest, final Account account) {
    setAccount(containerRequest, ContainerRequestUtil.AccountInfo.fromAccount(account));
  }

  private static void setAccount(final ContainerRequest containerRequest, final ContainerRequestUtil.AccountInfo info) {
    containerRequest.setProperty(ACCOUNT_UUID, info.accountId());
    containerRequest.setProperty(LINKED_DEVICE_IDS, info.deviceIds());
  }

  @Override
  public List<Pair<UUID, Byte>> handleRequestFinished(final RequestEvent requestEvent) {
    // Now that the request is finished, check whether the set of linked devices has changed. If the value did change or
    // if a devices was added or removed, all devices must disconnect and reauthenticate.
    if (requestEvent.getContainerRequest().getProperty(LINKED_DEVICE_IDS) != null) {

      @SuppressWarnings("unchecked") final Set<Byte> initialLinkedDeviceIds =
          (Set<Byte>) requestEvent.getContainerRequest().getProperty(LINKED_DEVICE_IDS);

      return accountsManager.getByAccountIdentifier((UUID) requestEvent.getContainerRequest().getProperty(ACCOUNT_UUID))
          .map(ContainerRequestUtil.AccountInfo::fromAccount)
          .map(accountInfo -> {
            final Set<Byte> deviceIdsToDisplace;
            final Set<Byte> currentLinkedDeviceIds = accountInfo.deviceIds();

            if (!initialLinkedDeviceIds.equals(currentLinkedDeviceIds)) {
              deviceIdsToDisplace = new HashSet<>(initialLinkedDeviceIds);
              deviceIdsToDisplace.addAll(currentLinkedDeviceIds);
            } else {
              deviceIdsToDisplace = Collections.emptySet();
            }

            return deviceIdsToDisplace.stream()
                .map(deviceId -> new Pair<>(accountInfo.accountId(), deviceId))
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
