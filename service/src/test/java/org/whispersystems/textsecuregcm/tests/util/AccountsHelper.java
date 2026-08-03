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
import jakarta.annotation.Nullable;
import java.io.IOException;
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
import org.signal.libsignal.zkgroup.InvalidInputException;
import org.signal.libsignal.zkgroup.VerificationFailedException;
import org.signal.libsignal.zkgroup.receipts.ReceiptCredentialPresentation;
import org.whispersystems.textsecuregcm.entities.AccountAttributes;
import org.whispersystems.textsecuregcm.entities.DeviceAttributes;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.identity.PniServiceIdentifier;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.DeviceIdentityInfo;
import org.whispersystems.textsecuregcm.storage.DeviceSpec;
import org.whispersystems.textsecuregcm.storage.ReceiptAlreadyRedeemedException;
import org.whispersystems.textsecuregcm.storage.ReceiptCredentialTestUtil;
import org.whispersystems.textsecuregcm.util.SystemMapper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

public class AccountsHelper {

  public static Account generateTestAccount(@Nullable String number, List<Device> devices) {
    return generateTestAccount(number, UUID.randomUUID(), UUID.randomUUID(), devices, null);
  }

  public static Account generateTestAccount(@Nullable String number, UUID uuid, final @Nullable UUID phoneNumberIdentifier, List<Device> devices, byte[] unidentifiedAccessKey) {
    return generateTestAccount(number, uuid, phoneNumberIdentifier, devices, unidentifiedAccessKey, TestRandomUtil.nextBytes(16));
  }

  public static Account generateTestAccount(@Nullable String number, UUID uuid, final @Nullable UUID phoneNumberIdentifier, List<Device> devices, byte[] unidentifiedAccessKey, final byte[] accountRecoveryPassword) {
    final Account account = new Account();
    account.setNumber(number, phoneNumberIdentifier);
    account.setAccountIdentifier(uuid);
    devices.forEach(account::addDevice);
    account.setUnidentifiedAccessKey(unidentifiedAccessKey);
    account.setAccountRecoveryPassword(accountRecoveryPassword);

    return account;
  }

  public static Account generateTestAccountNoPhoneNumber(List<Device> devices) {
    final Account account = new Account();
    account.setAccountIdentifier(UUID.randomUUID());
    devices.forEach(account::addDevice);

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

    when(mockAccountsManager.update(any(UUID.class), any(), any())).thenAnswer(invocation -> {
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
    if (account.getAccountIdentifier() != null || account.getIdentifier(IdentityType.ACI) != null) {
      final UUID accountIdentifier =
          Objects.requireNonNullElseGet(account.getIdentifier(IdentityType.ACI), account::getAccountIdentifier);

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
          case "getAccountIdentifier" -> when(updatedAccount.getAccountIdentifier()).thenAnswer(stubbing);
          case "getPhoneNumberIdentifierOptional" -> when(updatedAccount.getPhoneNumberIdentifierOptional()).thenAnswer(stubbing);
          case "getPhoneNumberIdentifier" -> when(updatedAccount.getPhoneNumberIdentifier()).thenAnswer(stubbing);
          case "getIdentifier" -> when(updatedAccount.getIdentifier(stubbing.getInvocation().getArgument(0))).thenAnswer(stubbing);
          case "isIdentifiedBy" -> when(updatedAccount.isIdentifiedBy(stubbing.getInvocation().getArgument(0))).thenAnswer(stubbing);
          case "getNumber" -> when(updatedAccount.getNumber()).thenAnswer(stubbing);
          case "getNumberOptional" -> when(updatedAccount.getNumberOptional()).thenAnswer(stubbing);
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
          case "getRecoveryPassword" -> when(updatedAccount.getAccountRecoveryPassword()).thenAnswer(stubbing);
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

  public static Account createAccount(final AccountsManager accountsManager, final String e164) {
    return new AccountBuilder(accountsManager).e164(e164).build();
  }

  public static class AccountBuilder {

    private final AccountsManager accountsManager;

    @Nullable
    private String e164;

    @Nullable
    private AccountAttributes accountAttributes;

    public AccountBuilder(final AccountsManager accountsManager) {
      this.accountsManager = accountsManager;
    }

    public AccountBuilder e164(@Nullable final String e164) {
      this.e164 = e164;
      return this;
    }

    public AccountBuilder accountAttributes(final AccountAttributes accountAttributes) {
      this.accountAttributes = accountAttributes;
      return this;
    }

    public Account build() {
      final AccountAttributes accountAttributes = this.accountAttributes != null
          ? this.accountAttributes
          : new AccountAttributes()
              .setDeviceAttributes(new DeviceAttributes(false, 1, e164 != null ? 1 : null, new byte[0], Collections.emptySet()))
              .setRecoveryPassword(TestRandomUtil.nextBytes(32));

      final ECKeyPair aciKeyPair = ECKeyPair.generate();
      final ECKeyPair pniKeyPair = ECKeyPair.generate();

      final DeviceSpec primaryDeviceSpec = new DeviceSpec(
          accountAttributes.getName(),
          "password",
          "OWT",
          accountAttributes.getCapabilities(),
          new DeviceIdentityInfo(
              accountAttributes.getRegistrationId(),
              KeysHelper.signedECPreKey(1, aciKeyPair),
              KeysHelper.signedKEMPreKey(3, aciKeyPair)),
          Optional.ofNullable(e164).map(_ -> new DeviceIdentityInfo(
              accountAttributes.getPhoneNumberIdentityRegistrationId()
                  .orElseThrow(() -> new AssertionError("Missing PNI registration ID")),
              KeysHelper.signedECPreKey(2, pniKeyPair),
              KeysHelper.signedKEMPreKey(4, pniKeyPair))),
          accountAttributes.getFetchesMessages(),
          Optional.empty(),
          Optional.empty());

      if (e164 != null) {
        return accountsManager.create(e164,
            accountAttributes,
            new IdentityKey(aciKeyPair.getPublicKey()),
            new IdentityKey(pniKeyPair.getPublicKey()),
            primaryDeviceSpec,
            null);
      } else {
        try {
          return accountsManager.create(accountAttributes,
              new IdentityKey(aciKeyPair.getPublicKey()),
              generateReceiptCredentialPresentation(),
              primaryDeviceSpec,
              null);
        } catch (ReceiptAlreadyRedeemedException e) {
          throw new RuntimeException(e);
        }
      }
    }

    private static ReceiptCredentialPresentation generateReceiptCredentialPresentation() {
      try {
        return ReceiptCredentialTestUtil.receiptPresentation();
      } catch (final InvalidInputException | VerificationFailedException e) {
        throw new AssertionError("Failed to generate receipt credential presentation", e);
      }
    }
  }
}
