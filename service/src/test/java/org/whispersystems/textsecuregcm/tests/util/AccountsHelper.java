/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockingDetails;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import org.junit.platform.commons.util.StringUtils;
import org.mockito.MockingDetails;
import org.mockito.stubbing.Stubbing;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.DeviceAttributes;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
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
   *    <li>{@link AccountsManager#update(UUID, Consumer)}</li>
   *    <li>{@link AccountsManager#updateDevice(UUID, byte, Consumer)}</li>
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
    when(mockAccountsManager.update(any(UUID.class), any())).thenAnswer(invocation -> {
      final UUID accountIdentifier = invocation.getArgument(0, UUID.class);
      final Account account = mockAccountsManager.getByAccountIdentifier(accountIdentifier).orElseThrow();

      for (int i = 0; i < retryCount; i++) {
        invocation.getArgument(1, Consumer.class).accept(account);
      }

      return account;
    });

    when(mockAccountsManager.update(any(Account.class), any())).thenAnswer(invocation -> {
      final Account account = invocation.getArgument(0);

      for (int i = 0; i < retryCount; i++) {
        invocation.getArgument(1, Consumer.class).accept(account);
      }

      return copyAndMarkStale(account);
    });

    when(mockAccountsManager.updateDevice(any(UUID.class), anyByte(), any())).thenAnswer(answer -> {
      final UUID accountIdentifier = answer.getArgument(0, UUID.class);
      final Account account = mockAccountsManager.getByAccountIdentifier(accountIdentifier).orElseThrow();

      final byte deviceId = answer.getArgument(1, Byte.class);

      for (int i = 0; i < retryCount; i++) {
        account.getDevice(deviceId).ifPresent(answer.getArgument(2, Consumer.class));
      }

      return account;
    });
  }

  @SuppressWarnings("unchecked")
  private static void setupMockUpdate(final AccountsManager mockAccountsManager, final boolean markStale) {
    when(mockAccountsManager.update(any(UUID.class), any())).thenAnswer(invocation -> {
      final UUID accountIdentifier = invocation.getArgument(0, UUID.class);
      final Account account = mockAccountsManager.getByAccountIdentifier(accountIdentifier).orElseThrow();

      invocation.getArgument(1, Consumer.class).accept(account);

      return account;
    });

    when(mockAccountsManager.update(any(Account.class), any())).thenAnswer(invocation -> {
      final Account account = invocation.getArgument(0);

      invocation.getArgument(1, Consumer.class).accept(account);

      return markStale ? copyAndMarkStale(account) : account;
    });

    when(mockAccountsManager.updateDevice(any(), anyByte(), any())).thenAnswer(invocation -> {
      final UUID accountIdentifier = invocation.getArgument(0, UUID.class);
      final Account account = mockAccountsManager.getByAccountIdentifier(accountIdentifier).orElseThrow();

      final byte deviceId = invocation.getArgument(1, Byte.class);
      account.getDevice(deviceId).ifPresent(invocation.getArgument(2, Consumer.class));

      return account;
    });

    when(mockAccountsManager.updateDeviceLastSeen(any(), any(), anyLong())).thenAnswer(invocation -> {
      final UUID accountIdentifier = invocation.getArgument(0, UUID.class);
      final Account account = mockAccountsManager.getByAccountIdentifier(accountIdentifier).orElseThrow();

      final Device device = account.getDevice(invocation.getArgument(1, Device.class).getId()).orElseThrow();
      device.setLastSeen(invocation.getArgument(2, Long.class));

      return mockAccountsManager.update(accountIdentifier, _ -> {});
    });
  }

  public static void setupMockGet(final AccountsManager mockAccountsManager, final Account account) {
    if (account.getUuid() != null || account.getIdentifier(IdentityType.ACI) != null) {
      final UUID accountIdentifier =
          Objects.requireNonNullElseGet(account.getIdentifier(IdentityType.ACI), account::getUuid);

      when(mockAccountsManager.getByAccountIdentifier(accountIdentifier))
          .thenReturn(Optional.of(account));

      when(mockAccountsManager.getByAccountIdentifierAsync(accountIdentifier))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

      when(mockAccountsManager.getByServiceIdentifier(new AciServiceIdentifier(accountIdentifier)))
          .thenReturn(Optional.of(account));

      when(mockAccountsManager.getByServiceIdentifierAsync(new AciServiceIdentifier(accountIdentifier)))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));
    }

    if (account.getPhoneNumberIdentifier() != null || account.getIdentifier(IdentityType.PNI) != null) {
      final UUID phoneNumberIdentifier =
          Objects.requireNonNullElseGet(account.getIdentifier(IdentityType.PNI), account::getPhoneNumberIdentifier);

      when(mockAccountsManager.getByPhoneNumberIdentifier(phoneNumberIdentifier))
          .thenReturn(Optional.of(account));

      when(mockAccountsManager.getByPhoneNumberIdentifierAsync(phoneNumberIdentifier))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));

      when(mockAccountsManager.getByServiceIdentifier(new PniServiceIdentifier(phoneNumberIdentifier)))
          .thenReturn(Optional.of(account));

      when(mockAccountsManager.getByServiceIdentifierAsync(new PniServiceIdentifier(phoneNumberIdentifier)))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));
    }

    if (StringUtils.isNotBlank(account.getNumber())) {
      when(mockAccountsManager.getByE164(account.getNumber())).thenReturn(Optional.of(account));
    }

    account.getUsernameHash().ifPresent(usernameHash -> when(mockAccountsManager.getByUsernameHash(usernameHash))
        .thenReturn(CompletableFuture.completedFuture(Optional.of(account))));

    if (account.getUsernameLinkHandle() != null) {
      when(mockAccountsManager.getByUsernameLinkHandle(account.getUsernameLinkHandle()))
          .thenReturn(CompletableFuture.completedFuture(Optional.of(account)));
    }
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

  public static Account createAccount(final AccountsManager accountsManager, final String e164)
      throws InterruptedException {

    return createAccount(accountsManager, e164, new AccountAttributes().setDeviceAttributes(
        new DeviceAttributes(false, 1, 1, new byte[0], Collections.emptySet())));
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
