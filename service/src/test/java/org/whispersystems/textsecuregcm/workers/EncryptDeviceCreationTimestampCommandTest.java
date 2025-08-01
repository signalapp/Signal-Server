/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.auth.UnidentifiedAccessUtil;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.tests.util.AccountsHelper;
import org.whispersystems.textsecuregcm.tests.util.DevicesHelper;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import reactor.core.publisher.Flux;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EncryptDeviceCreationTimestampCommandTest {

  private static class TestEncryptDeviceCreationTimestampCommand extends EncryptDeviceCreationTimestampCommand {

    private final CommandDependencies commandDependencies;
    private final Namespace namespace;

    public TestEncryptDeviceCreationTimestampCommand(final AccountsManager accountsManager, final boolean isDryRun) {
      super();

      commandDependencies = mock(CommandDependencies.class);
      when(commandDependencies.accountsManager()).thenReturn(accountsManager);

      namespace = mock(Namespace.class);
      when(namespace.getBoolean(EncryptDeviceCreationTimestampCommand.DRY_RUN_ARGUMENT)).thenReturn(isDryRun);
      when(namespace.getInt(EncryptDeviceCreationTimestampCommand.BUFFER_ARGUMENT)).thenReturn(5);
    }

    @Override
    protected CommandDependencies getCommandDependencies() {
      return commandDependencies;
    }

    @Override
    protected Namespace getNamespace() {
      return namespace;
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void crawlAccounts(final boolean isDryRun) {
    final String number = "+14152222222";
    final UUID uuid = UUID.randomUUID();

    final Account testAccount = AccountsHelper.generateTestAccount(number, uuid, UUID.randomUUID(), List.of(
        DevicesHelper.createDevice(Device.PRIMARY_ID)), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    testAccount.setIdentityKey(new IdentityKey(ECKeyPair.generate().getPublicKey()));

    final AccountsManager accountsManager = mock(AccountsManager.class);
    when(accountsManager.updateAsync(any(), any())).thenAnswer(invocation -> {
      final Account account = invocation.getArgument(0);
      final Consumer<Account> updater = invocation.getArgument(1);
      updater.accept(account);
      return CompletableFuture.completedFuture(account);
    });

    final EncryptDeviceCreationTimestampCommand encryptDeviceCreationTimestampCommand =
        new TestEncryptDeviceCreationTimestampCommand(accountsManager, isDryRun);

    encryptDeviceCreationTimestampCommand.crawlAccounts(Flux.just(testAccount));

    if (isDryRun) {
      verify(accountsManager, never()).updateAsync(any(), any());
      assertNull(testAccount.getDevices().getFirst().getCreatedAtCiphertext());
    } else {
      verify(accountsManager).updateAsync(eq(testAccount), any());
      assertNotNull(testAccount.getDevices().getFirst().getCreatedAtCiphertext());
    }
  }

  @Test
  void crawlAccountsWithEncryptedTimestamps() {
    final Account unencryptedTimestampAccount = AccountsHelper.generateTestAccount("+14152222222", UUID.randomUUID(), UUID.randomUUID(), List.of(
        DevicesHelper.createDevice(Device.PRIMARY_ID)), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    unencryptedTimestampAccount.setIdentityKey(new IdentityKey(ECKeyPair.generate().getPublicKey()));

    final Account encryptedTimestampAccount = AccountsHelper.generateTestAccount("+14152222223", UUID.randomUUID(), UUID.randomUUID(), List.of(
        DevicesHelper.createDevice(Device.PRIMARY_ID)), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    encryptedTimestampAccount.setIdentityKey(new IdentityKey(ECKeyPair.generate().getPublicKey()));
    encryptedTimestampAccount.getDevices().getFirst().setCreatedAtCiphertext(TestRandomUtil.nextBytes(56));

    final Account halfEncryptedTimestampAccount = AccountsHelper.generateTestAccount("+14152222224", UUID.randomUUID(), UUID.randomUUID(), List.of(
        DevicesHelper.createDevice(Device.PRIMARY_ID), DevicesHelper.createDevice((byte) 2)), new byte[UnidentifiedAccessUtil.UNIDENTIFIED_ACCESS_KEY_LENGTH]);
    halfEncryptedTimestampAccount.setIdentityKey(new IdentityKey(ECKeyPair.generate().getPublicKey()));
    halfEncryptedTimestampAccount.getDevices().getFirst().setCreatedAtCiphertext(TestRandomUtil.nextBytes(56));

    final AccountsManager accountsManager = mock(AccountsManager.class);
    when(accountsManager.updateAsync(any(), any())).thenAnswer(invocation -> {
      final Account account = invocation.getArgument(0);
      final Consumer<Account> updater = invocation.getArgument(1);
      updater.accept(account);
      return CompletableFuture.completedFuture(account);
    });

    final EncryptDeviceCreationTimestampCommand encryptDeviceCreationTimestampCommand =
        new TestEncryptDeviceCreationTimestampCommand(accountsManager, false);

    assertNull(unencryptedTimestampAccount.getDevices().getFirst().getCreatedAtCiphertext());
    assertNull(halfEncryptedTimestampAccount.getDevices().get(1).getCreatedAtCiphertext());

    encryptDeviceCreationTimestampCommand.crawlAccounts(Flux.just(unencryptedTimestampAccount,
        encryptedTimestampAccount,
        halfEncryptedTimestampAccount));

    verify(accountsManager, times(1)).updateAsync(eq(unencryptedTimestampAccount), any());
    verify(accountsManager, never()).updateAsync(eq(encryptedTimestampAccount), any());
    verify(accountsManager, times(1)).updateAsync(eq(halfEncryptedTimestampAccount), any());

    assertNotNull(unencryptedTimestampAccount.getDevices().getFirst().getCreatedAtCiphertext());
    assertNotNull(halfEncryptedTimestampAccount.getDevices().get(1).getCreatedAtCiphertext());
  }
}
