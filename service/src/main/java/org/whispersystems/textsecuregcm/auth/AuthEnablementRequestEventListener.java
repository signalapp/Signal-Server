/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.ws.rs.core.SecurityContext;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEvent.Type;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.push.ClientPresenceManager;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

/**
 * This {@link RequestEventListener} observes intra-request changes in {@link Account#isEnabled()} and {@link
 * Device#isEnabled()}.
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
public class AuthEnablementRequestEventListener implements RequestEventListener {

  private static final Logger logger = LoggerFactory.getLogger(AuthEnablementRequestEventListener.class);

  private static final String ACCOUNT_ENABLED = AuthEnablementRequestEventListener.class.getName() + ".accountEnabled";
  private static final String DEVICES_ENABLED = AuthEnablementRequestEventListener.class.getName() + ".devicesEnabled";

  private static final Counter DISPLACED_ACCOUNTS = Metrics.counter(
      name(AuthEnablementRequestEventListener.class, "displacedAccounts"));
  private static final Counter DISPLACED_DEVICES = Metrics.counter(
      name(AuthEnablementRequestEventListener.class, "displacedDevices"));

  private final ClientPresenceManager clientPresenceManager;

  public AuthEnablementRequestEventListener(final ClientPresenceManager clientPresenceManager) {
    this.clientPresenceManager = clientPresenceManager;
  }

  @Override
  public void onEvent(final RequestEvent event) {

    if (event.getType() == Type.REQUEST_FILTERED) {
      // The authenticated principal, if any, will be available after filters have run.
      // Now that the account is known, capture a snapshot of `isEnabled` for the account and its devices,
      // before carrying out the request’s business logic.
      findAccount(event.getContainerRequest())
          .ifPresent(
              account -> {
                event.getContainerRequest().setProperty(ACCOUNT_ENABLED, account.isEnabled());
                event.getContainerRequest().setProperty(DEVICES_ENABLED, buildDevicesEnabledMap(account));
              });

    } else if (event.getType() == Type.FINISHED) {
      // Now that the request is finished, check whether `isEnabled` changed for any of the devices, or the account
      // as a whole. If the value did change, the affected device(s) must disconnect and reauthenticate.
      // If a device was removed, it must also disconnect.
      if (event.getContainerRequest().getProperty(ACCOUNT_ENABLED) != null &&
          event.getContainerRequest().getProperty(DEVICES_ENABLED) != null) {

        final boolean accountInitiallyEnabled = (boolean) event.getContainerRequest().getProperty(ACCOUNT_ENABLED);
        @SuppressWarnings("unchecked") final Map<Long, Boolean> initialDevicesEnabled = (Map<Long, Boolean>) event.getContainerRequest()
            .getProperty(DEVICES_ENABLED);

        findAccount(event.getContainerRequest()).ifPresentOrElse(account -> {
              final Set<Long> deviceIdsToDisplace;

              if (account.isEnabled() != accountInitiallyEnabled) {
                // the @Auth for all active connections must change when account.isEnabled() changes
                deviceIdsToDisplace = account.getDevices().stream()
                    .map(Device::getId).collect(Collectors.toSet());

                deviceIdsToDisplace.addAll(initialDevicesEnabled.keySet());

                DISPLACED_ACCOUNTS.increment();

              } else if (!initialDevicesEnabled.isEmpty()) {

                deviceIdsToDisplace = new HashSet<>();
                final Map<Long, Boolean> currentDevicesEnabled = buildDevicesEnabledMap(account);

                initialDevicesEnabled.forEach((deviceId, enabled) -> {
                  // `null` indicates the device was removed from the account. Any active presence should be removed.
                  final boolean enabledMatches = Objects.equals(enabled,
                      currentDevicesEnabled.getOrDefault(deviceId, null));

                  if (!enabledMatches) {
                    deviceIdsToDisplace.add(deviceId);

                    DISPLACED_DEVICES.increment();
                  }
                });
              } else {
                deviceIdsToDisplace = Collections.emptySet();
              }

              deviceIdsToDisplace.forEach(deviceId -> {
                try {
                  // displacing presence will cause a reauthorization for the device’s active connections
                  clientPresenceManager.displacePresence(account.getUuid(), deviceId);
                } catch (final Exception e) {
                  logger.error("Could not displace device presence", e);
                }
              });
            },
            () -> logger.error("Request had account, but it is no longer present")
        );
      }
    }
  }

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
}
