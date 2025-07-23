/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.mockito.MockingDetails;
import org.mockito.stubbing.Stubbing;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.SaltedTokenHash;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DeviceSpec;
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

  /**
   * Sets up stubbing for:
   * <ul>
   *    <li>{@link AccountsManager#update(Account, Consumer)}</li>
   *    <li>{@link AccountsManager#updateAsync(Account, Consumer)}</li>
   *    <li>{@link AccountsManager#updateDevice(Account, byte, Consumer)}</li>
   *    <li>{@link AccountsManager#updateDeviceAsync(Account, byte, Consumer)}</li>
   * </ul>
   *
   * with multiple calls to the {@link Consumer<Account>}. This simulates retries from {@link org.whispersystems.textsecuregcm.storage.ContestedOptimisticLockException}.
   * Callers will typically set up stubbing for relevant {@link Account} methods with multiple {@link org.mockito.stubbing.OngoingStubbing#thenReturn(Object)}
   * calls:
   * <pre>
   *   // example stubbing
   *   when(account.getNextDeviceId())
   *     .thenReturn(2)
   *     .thenReturn(3);
   * </pre>
   */
  @SuppressWarnings("unchecked")
  public static void setupMockUpdateWithRetries(final AccountsManager mockAccountsManager, final int retryCount) {
    when(mockAccountsManager.update(any(), any())).thenAnswer(answer -> {
      final Account account = answer.getArgument(0, Account.class);

      for (int i = 0; i < retryCount; i++) {
        answer.getArgument(1, Consumer.class).accept(account);
      }

      return copyAndMarkStale(account);
    });

    when(mockAccountsManager.updateAsync(any(), any())).thenAnswer(answer -> {
      final Account account = answer.getArgument(0, Account.class);

      for (int i = 0; i < retryCount; i++) {
        answer.getArgument(1, Consumer.class).accept(account);
      }

      return CompletableFuture.completedFuture(copyAndMarkStale(account));
    });

    when(mockAccountsManager.updateDevice(any(), anyByte(), any())).thenAnswer(answer -> {
      final Account account = answer.getArgument(0, Account.class);
      final byte deviceId = answer.getArgument(1, Byte.class);

      for (int i = 0; i < retryCount; i++) {
        account.getDevice(deviceId).ifPresent(answer.getArgument(2, Consumer.class));
      }

      return copyAndMarkStale(account);
    });

    when(mockAccountsManager.updateDeviceAsync(any(), anyByte(), any())).thenAnswer(answer -> {
      final Account account = answer.getArgument(0, Account.class);
      final byte deviceId = answer.getArgument(1, Byte.class);

      for (int i = 0; i < retryCount; i++) {
        account.getDevice(deviceId).ifPresent(answer.getArgument(2, Consumer.class));
      }

      return CompletableFuture.completedFuture(copyAndMarkStale(account));
    });
  }

  @SuppressWarnings("unchecked")
  private static void setupMockUpdate(final AccountsManager mockAccountsManager, final boolean markStale) {
    when(mockAccountsManager.update(any(), any())).thenAnswer(answer -> {
      final Account account = answer.getArgument(0, Account.class);
      answer.getArgument(1, Consumer.class).accept(account);

      return markStale ? copyAndMarkStale(account) : account;
    });

    when(mockAccountsManager.updateAsync(any(), any())).thenAnswer(answer -> {
      final Account account = answer.getArgument(0, Account.class);
      answer.getArgument(1, Consumer.class).accept(account);

      return CompletableFuture.completedFuture(markStale ? copyAndMarkStale(account) : account);
    });

    when(mockAccountsManager.updateDevice(any(), anyByte(), any())).thenAnswer(answer -> {
      final Account account = answer.getArgument(0, Account.class);
      final byte deviceId = answer.getArgument(1, Byte.class);
      account.getDevice(deviceId).ifPresent(answer.getArgument(2, Consumer.class));

      return markStale ? copyAndMarkStale(account) : account;
    });

    when(mockAccountsManager.updateDeviceAsync(any(), anyByte(), any())).thenAnswer(answer -> {
      final Account account = answer.getArgument(0, Account.class);
      final byte deviceId = answer.getArgument(1, Byte.class);
      account.getDevice(deviceId).ifPresent(answer.getArgument(2, Consumer.class));

      return CompletableFuture.completedFuture(markStale ? copyAndMarkStale(account) : account);
    });

    when(mockAccountsManager.updateDeviceLastSeen(any(), any(), anyLong())).thenAnswer(answer -> {
      answer.getArgument(1, Device.class).setLastSeen(answer.getArgument(2, Long.class));
      return mockAccountsManager.update(answer.getArgument(0, Account.class), account -> {});
    });

    when(mockAccountsManager.updateDeviceAuthentication(any(), any(), any())).thenAnswer(answer -> {
      answer.getArgument(1, Device.class).setAuthTokenHash(answer.getArgument(2, SaltedTokenHash.class));
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
          case "getIdentifier" -> when(updatedAccount.getIdentifier(stubbing.getInvocation().getArgument(0))).thenAnswer(stubbing);
          case "isIdentifiedBy" -> when(updatedAccount.isIdentifiedBy(stubbing.getInvocation().getArgument(0))).thenAnswer(stubbing);
          case "getNumber" -> when(updatedAccount.getNumber()).thenAnswer(stubbing);
          case "getUsername" -> when(updatedAccount.getUsernameHash()).thenAnswer(stubbing);
          case "getUsernameHash" -> when(updatedAccount.getUsernameHash()).thenAnswer(stubbing);
          case "getUsernameLinkHandle" -> when(updatedAccount.getUsernameLinkHandle()).thenAnswer(stubbing);
          case "getDevices" -> when(updatedAccount.getDevices()).thenAnswer(stubbing);
          case "getDevice" -> when(updatedAccount.getDevice(stubbing.getInvocation().getArgument(0))).thenAnswer(stubbing);
          case "getPrimaryDevice" -> when(updatedAccount.getPrimaryDevice()).thenAnswer(stubbing);
          case "isDiscoverableByPhoneNumber" -> when(updatedAccount.isDiscoverableByPhoneNumber()).thenAnswer(stubbing);
          case "getNextDeviceId" -> when(updatedAccount.getNextDeviceId()).thenAnswer(stubbing);
          case "hasCapability" -> when(updatedAccount.hasCapability(stubbing.getInvocation().getArgument(0))).thenAnswer(stubbing);
          case "getRegistrationLock" -> when(updatedAccount.getRegistrationLock()).thenAnswer(stubbing);
          case "getIdentityKey" ->
              when(updatedAccount.getIdentityKey(stubbing.getInvocation().getArgument(0))).thenAnswer(stubbing);
          case "getBadges" -> when(updatedAccount.getBadges()).thenAnswer(stubbing);
          case "getBackupVoucher" -> when(updatedAccount.getBackupVoucher()).thenAnswer(stubbing);
          case "getLastSeen" -> when(updatedAccount.getLastSeen()).thenAnswer(stubbing);
          case "hasLockedCredentials" -> when(updatedAccount.hasLockedCredentials()).thenAnswer(stubbing);
          case "getCurrentProfileVersion" -> when(updatedAccount.getCurrentProfileVersion()).thenAnswer(stubbing);
          case "getUnidentifiedAccessKey" -> when(updatedAccount.getUnidentifiedAccessKey()).thenAnswer(stubbing);
          default -> throw new IllegalArgumentException("unsupported method: Account#" + stubbing.getInvocation().getMethod().getName());
        }
      }

    } else {
      final ObjectMapper mapper = SystemMapper.jsonMapper();
      updatedAccount = mapper.readValue(mapper.writeValueAsBytes(account), Account.class);
      updatedAccount.setNumber(account.getNumber(), account.getPhoneNumberIdentifier());
      account.markStale();
    }

    return updatedAccount;
  }

  public static Account eqUuid(Account value) {
    return argThat(other -> other.getUuid().equals(value.getUuid()));
  }

  public static Account createAccount(final AccountsManager accountsManager, final String e164)
      throws InterruptedException {

    return createAccount(accountsManager, e164, new AccountAttributes());
  }

  public static Account createAccount(final AccountsManager accountsManager, final String e164, final AccountAttributes accountAttributes)
      throws InterruptedException {

    return createAccount(accountsManager, e164, accountAttributes, ECKeyPair.generate(), ECKeyPair.generate());
  }

  public static Account createAccount(final AccountsManager accountsManager,
      final String e164,
      final AccountAttributes accountAttributes,
      final ECKeyPair aciKeyPair,
      final ECKeyPair pniKeyPair) throws InterruptedException {

    return accountsManager.create(e164,
        accountAttributes,
        new ArrayList<>(),
        new IdentityKey(aciKeyPair.getPublicKey()),
        new IdentityKey(pniKeyPair.getPublicKey()),
        new DeviceSpec(
            accountAttributes.getName(),
            "password",
            "OWT",
            accountAttributes.getCapabilities(),
            accountAttributes.getRegistrationId(),
            accountAttributes.getPhoneNumberIdentityRegistrationId(),
            accountAttributes.getFetchesMessages(),
            Optional.empty(),
            Optional.empty(),
            KeysHelper.signedECPreKey(1, aciKeyPair),
            KeysHelper.signedECPreKey(2, pniKeyPair),
            KeysHelper.signedKEMPreKey(3, aciKeyPair),
            KeysHelper.signedKEMPreKey(4, pniKeyPair)),
        null);
  }
}
