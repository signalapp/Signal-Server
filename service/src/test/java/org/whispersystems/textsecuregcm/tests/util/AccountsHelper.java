/*
 * Copyright 2013-2022 Signal Messenger, LLC
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
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import org.mockito.MockingDetails;
import org.mockito.stubbing.Stubbing;
import org.whispersystems.textsecuregcm.auth.AuthenticationCredentials;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.SystemMapper;

public class AccountsHelper {

  public static Account generateTestAccount(String number, List<Device> devices) {
    return generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), devices, null);
  }

  public static Account generateTestAccount(String number, UUID uuid, final UUID phoneNumberIdentifier, List<Device> devices, byte[] unidentifiedAccessKey) {
    final Account account = new Account();
    account.setNumber(number, phoneNumberIdentifier);
    account.setUuid(uuid);
    devices.forEach(account::addDevice);
    account.setUnidentifiedAccessKey(unidentifiedAccessKey);

    return account;
  }

  public static void setupMockUpdate(final AccountsManager mockAccountsManager) {
    setupMockUpdate(mockAccountsManager, true);
  }

  /**
   * Only for use by {@link AuthHelper}
   */
  public static void setupMockUpdateForAuthHelper(final AccountsManager mockAccountsManager) {
    setupMockUpdate(mockAccountsManager, false);
  }

  private static void setupMockUpdate(final AccountsManager mockAccountsManager, final boolean markStale) {
    when(mockAccountsManager.update(any(), any())).thenAnswer(answer -> {
      final Account account = answer.getArgument(0, Account.class);
      answer.getArgument(1, Consumer.class).accept(account);

      return markStale ? copyAndMarkStale(account) : account;
    });

    when(mockAccountsManager.updateDevice(any(), anyLong(), any())).thenAnswer(answer -> {
      final Account account = answer.getArgument(0, Account.class);
      final Long deviceId = answer.getArgument(1, Long.class);
      account.getDevice(deviceId).ifPresent(answer.getArgument(2, Consumer.class));

      return markStale ? copyAndMarkStale(account) : account;
    });

    when(mockAccountsManager.updateDeviceLastSeen(any(), any(), anyLong())).thenAnswer(answer -> {
      answer.getArgument(1, Device.class).setLastSeen(answer.getArgument(2, Long.class));
      return mockAccountsManager.update(answer.getArgument(0, Account.class), account -> {});
    });

    when(mockAccountsManager.updateDeviceAuthentication(any(), any(), any())).thenAnswer(answer -> {
      answer.getArgument(1, Device.class).setAuthenticationCredentials(answer.getArgument(2, AuthenticationCredentials.class));
      return mockAccountsManager.update(answer.getArgument(0, Account.class), account -> {});
    });
  }

  public static void setupMockGet(final AccountsManager mockAccountsManager, final Set<Account> mockAccounts) {
    when(mockAccountsManager.getByAccountIdentifier(any(UUID.class))).thenAnswer(answer -> {

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
          case "getUuid" -> when(updatedAccount.getUuid()).thenAnswer(stubbing);
          case "getPhoneNumberIdentifier" -> when(updatedAccount.getPhoneNumberIdentifier()).thenAnswer(stubbing);
          case "getNumber" -> when(updatedAccount.getNumber()).thenAnswer(stubbing);
          case "getUsername" -> when(updatedAccount.getUsername()).thenAnswer(stubbing);
          case "getDevices" -> when(updatedAccount.getDevices()).thenAnswer(stubbing);
          case "getDevice" -> when(updatedAccount.getDevice(stubbing.getInvocation().getArgument(0))).thenAnswer(stubbing);
          case "getMasterDevice" -> when(updatedAccount.getMasterDevice()).thenAnswer(stubbing);
          case "isEnabled" -> when(updatedAccount.isEnabled()).thenAnswer(stubbing);
          case "isDiscoverableByPhoneNumber" -> when(updatedAccount.isDiscoverableByPhoneNumber()).thenAnswer(stubbing);
          case "getNextDeviceId" -> when(updatedAccount.getNextDeviceId()).thenAnswer(stubbing);
          case "isSenderKeySupported" -> when(updatedAccount.isSenderKeySupported()).thenAnswer(stubbing);
          case "isAnnouncementGroupSupported" -> when(updatedAccount.isAnnouncementGroupSupported()).thenAnswer(stubbing);
          case "isChangeNumberSupported" -> when(updatedAccount.isChangeNumberSupported()).thenAnswer(stubbing);
          case "isPniSupported" -> when(updatedAccount.isPniSupported()).thenAnswer(stubbing);
          case "isStoriesSupported" -> when(updatedAccount.isStoriesSupported()).thenAnswer(stubbing);
          case "isGiftBadgesSupported" -> when(updatedAccount.isGiftBadgesSupported()).thenAnswer(stubbing);
          case "isPaymentActivationSupported" -> when(updatedAccount.isPaymentActivationSupported()).thenAnswer(stubbing);
          case "getEnabledDeviceCount" -> when(updatedAccount.getEnabledDeviceCount()).thenAnswer(stubbing);
          case "getRegistrationLock" -> when(updatedAccount.getRegistrationLock()).thenAnswer(stubbing);
          case "getIdentityKey" -> when(updatedAccount.getIdentityKey()).thenAnswer(stubbing);
          case "getBadges" -> when(updatedAccount.getBadges()).thenAnswer(stubbing);
          case "getLastSeen" -> when(updatedAccount.getLastSeen()).thenAnswer(stubbing);
          default -> throw new IllegalArgumentException("unsupported method: Account#" + stubbing.getInvocation().getMethod().getName());
        }
      }

    } else {
      final ObjectMapper mapper = SystemMapper.getMapper();
      updatedAccount = mapper.readValue(mapper.writeValueAsBytes(account), Account.class);
      updatedAccount.setNumber(account.getNumber(), account.getPhoneNumberIdentifier());
      account.markStale();
    }

    return updatedAccount;
  }

  public static Account eqUuid(Account value) {
    return argThat(other -> other.getUuid().equals(value.getUuid()));
  }

}
