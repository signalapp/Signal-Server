/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import org.glassfish.jersey.server.ContainerRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Pair;

import javax.annotation.Nullable;
import javax.ws.rs.core.SecurityContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class PhoneNumberChangeRefreshRequirementProviderTest {

  private PhoneNumberChangeRefreshRequirementProvider provider;

  private Account account;
  private ContainerRequest request;

  private static final UUID ACCOUNT_UUID = UUID.randomUUID();
  private static final String NUMBER = "+18005551234";
  private static final String CHANGED_NUMBER = "+18005554321";

  @BeforeEach
  void setUp() {
    provider = new PhoneNumberChangeRefreshRequirementProvider();

    account = mock(Account.class);
    final Device device = mock(Device.class);

    when(account.getUuid()).thenReturn(ACCOUNT_UUID);
    when(account.getNumber()).thenReturn(NUMBER);
    when(account.getDevices()).thenReturn(Set.of(device));
    when(device.getId()).thenReturn(Device.MASTER_ID);

    request = mock(ContainerRequest.class);

    final Map<String, Object> requestProperties = new HashMap<>();

    doAnswer(invocation -> {
      requestProperties.put(invocation.getArgument(0, String.class), invocation.getArgument(1));
      return null;
    }).when(request).setProperty(anyString(), any());

    when(request.getProperty(anyString())).thenAnswer(
        invocation -> requestProperties.get(invocation.getArgument(0, String.class)));
  }

  @Test
  void handleRequestNoChange() {
    setAuthenticatedAccount(request, account);

    provider.handleRequestFiltered(request);
    assertEquals(Collections.emptyList(), provider.handleRequestFinished(request));
  }

  @Test
  void handleRequestNumberChange() {
    setAuthenticatedAccount(request, account);

    provider.handleRequestFiltered(request);
    when(account.getNumber()).thenReturn(CHANGED_NUMBER);
    assertEquals(List.of(new Pair<>(ACCOUNT_UUID, Device.MASTER_ID)), provider.handleRequestFinished(request));
  }

  @Test
  void handleRequestNoAuthenticatedAccount() {
    final ContainerRequest request = mock(ContainerRequest.class);
    setAuthenticatedAccount(request, null);

    provider.handleRequestFiltered(request);
    assertEquals(Collections.emptyList(), provider.handleRequestFinished(request));
  }

  private void setAuthenticatedAccount(final ContainerRequest mockRequest, @Nullable final Account account) {
    final SecurityContext securityContext = mock(SecurityContext.class);

    when(mockRequest.getSecurityContext()).thenReturn(securityContext);

    if (account != null) {
      final AuthenticatedAccount authenticatedAccount = mock(AuthenticatedAccount.class);

      when(securityContext.getUserPrincipal()).thenReturn(authenticatedAccount);
      when(authenticatedAccount.getAccount()).thenReturn(account);
    } else {
      when(securityContext.getUserPrincipal()).thenReturn(null);
    }
  }
}
