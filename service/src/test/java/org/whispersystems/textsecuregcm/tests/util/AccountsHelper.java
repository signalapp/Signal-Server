/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.mockito.MockingDetails;
import org.mockito.stubbing.Stubbing;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.util.SystemMapper;

public class AccountsHelper {

  public static void setupMockUpdate(final AccountsManager mockAccountsManager) {
    when(mockAccountsManager.update(any(), any())).thenAnswer(answer -> {
      final Account account = answer.getArgument(0, Account.class);
      answer.getArgument(1, Consumer.class).accept(account);

      return copyAndMarkStale(account);
    });

    when(mockAccountsManager.updateDevice(any(), anyLong(), any())).thenAnswer(answer -> {
      final Account account = answer.getArgument(0, Account.class);
      final Long deviceId = answer.getArgument(1, Long.class);
      account.getDevice(deviceId).ifPresent(answer.getArgument(2, Consumer.class));

      return copyAndMarkStale(account);
    });
  }

  public static void setupMockGet(final AccountsManager mockAccountsManager, final Set<Account> mockAccounts) {
    when(mockAccountsManager.get(any(UUID.class))).thenAnswer(answer -> {

      final UUID uuid = answer.getArgument(0, UUID.class);

      return mockAccounts.stream()
          .filter(account -> uuid.equals(account.getUuid()))
          .findFirst()
          .map(account -> {
            try {
              return copyAndMarkStale(account);
            } catch (final Exception e) {
              throw new RuntimeException(e);
            }
          });
    });
  }

  private static Account copyAndMarkStale(Account account) throws IOException {
    MockingDetails mockingDetails = mockingDetails(account);

    final Account updatedAccount;
    if (mockingDetails.isMock()) {

      updatedAccount = mock(Account.class);

      // it’s not possible to make `account` behave as if it were stale, because we use static mocks in AuthHelper

      for (Stubbing stubbing : mockingDetails.getStubbings()) {
        switch (stubbing.getInvocation().getMethod().getName()) {
          case "getUuid": {
            when(updatedAccount.getUuid()).thenAnswer(stubbing);
            break;
          }
          case "getNumber": {
            when(updatedAccount.getNumber()).thenAnswer(stubbing);
            break;
          }
          case "getDevices": {
            when(updatedAccount.getDevices())
                .thenAnswer(stubbing);
            break;
          }
          case "getDevice": {
            when(updatedAccount.getDevice(stubbing.getInvocation().getArgument(0)))
                .thenAnswer(stubbing);
            break;
          }
          case "getMasterDevice": {
            when(updatedAccount.getMasterDevice()).thenAnswer(stubbing);
            break;
          }
          case "getAuthenticatedDevice": {
            when(updatedAccount.getAuthenticatedDevice()).thenAnswer(stubbing);
            break;
          }
          case "isEnabled": {
            when(updatedAccount.isEnabled()).thenAnswer(stubbing);
            break;
          }
          case "isDiscoverableByPhoneNumber": {
            when(updatedAccount.isDiscoverableByPhoneNumber()).thenAnswer(stubbing);
            break;
          }
          case "getNextDeviceId": {
            when(updatedAccount.getNextDeviceId()).thenAnswer(stubbing);
            break;
          }
          case "isGroupsV2Supported": {
            when(updatedAccount.isGroupsV2Supported()).thenAnswer(stubbing);
            break;
          }
          case "isGv1MigrationSupported": {
            when(updatedAccount.isGv1MigrationSupported()).thenAnswer(stubbing);
            break;
          }
          case "isSenderKeySupported": {
            when(updatedAccount.isSenderKeySupported()).thenAnswer(stubbing);
            break;
          }
          case "isAnnouncementGroupSupported": {
            when(updatedAccount.isAnnouncementGroupSupported()).thenAnswer(stubbing);
            break;
          }
          case "getEnabledDeviceCount": {
            when(updatedAccount.getEnabledDeviceCount()).thenAnswer(stubbing);
            break;
          }
          case "getRelay": {
            // TODO unused
            when(updatedAccount.getRelay()).thenAnswer(stubbing);
            break;
          }
          case "getRegistrationLock": {
            when(updatedAccount.getRegistrationLock()).thenAnswer(stubbing);
            break;
          }
          case "getIdentityKey": {
            when(updatedAccount.getIdentityKey()).thenAnswer(stubbing);
            break;
          }
          default: {
            throw new IllegalArgumentException(
                "unsupported method: Account#" + stubbing.getInvocation().getMethod().getName());
          }
        }
      }

    } else {
      final ObjectMapper mapper = SystemMapper.getMapper();
      updatedAccount = mapper.readValue(mapper.writeValueAsBytes(account), Account.class);
      updatedAccount.setNumber(account.getNumber());
      account.markStale();
    }


    return updatedAccount;
  }

  public static Account eqUuid(Account value) {
    return argThat(other -> other.getUuid().equals(value.getUuid()));
  }

}
