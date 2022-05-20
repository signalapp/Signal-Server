/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.util.Pair;

public class PhoneNumberChangeRefreshRequirementProvider implements WebsocketRefreshRequirementProvider {

  private static final String INITIAL_NUMBER_KEY =
      PhoneNumberChangeRefreshRequirementProvider.class.getName() + ".initialNumber";

  @Override
  public void handleRequestFiltered(final RequestEvent requestEvent) {
    ContainerRequestUtil.getAuthenticatedAccount(requestEvent.getContainerRequest())
        .ifPresent(account -> requestEvent.getContainerRequest().setProperty(INITIAL_NUMBER_KEY, account.getNumber()));
  }

  @Override
  public List<Pair<UUID, Long>> handleRequestFinished(final RequestEvent requestEvent) {
    final String initialNumber = (String) requestEvent.getContainerRequest().getProperty(INITIAL_NUMBER_KEY);

    if (initialNumber != null) {
      final Optional<Account> maybeAuthenticatedAccount =
          ContainerRequestUtil.getAuthenticatedAccount(requestEvent.getContainerRequest());

      return maybeAuthenticatedAccount
          .filter(account -> !initialNumber.equals(account.getNumber()))
          .map(account -> account.getDevices().stream()
              .map(device -> new Pair<>(account.getUuid(), device.getId()))
              .collect(Collectors.toList()))
          .orElse(Collections.emptyList());
    } else {
      return Collections.emptyList();
    }
  }
}
