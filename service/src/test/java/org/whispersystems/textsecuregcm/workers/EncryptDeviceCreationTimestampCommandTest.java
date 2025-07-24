/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedStatic;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.EncryptDeviceCreationTimestampUtil;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;
import reactor.core.publisher.Flux;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
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
    final Account account = mock(Account.class);
    final Device device = mock(Device.class);

    final IdentityKey identityKey = new IdentityKey(ECKeyPair.generate().getPublicKey());
    final byte deviceId = (byte) 1;
    final long createdAt = System.currentTimeMillis();
    final int registrationId = 123;

    when(account.getDevices()).thenReturn(List.of(device));
    when(account.getIdentityKey(IdentityType.ACI)).thenReturn(identityKey);
    when(device.getCreated()).thenReturn(createdAt);
    when(device.getId()).thenReturn(deviceId);
    when(device.getRegistrationId(IdentityType.ACI)).thenReturn(registrationId);

    final AccountsManager accountsManager = mock(AccountsManager.class);
    when(accountsManager.updateDeviceAsync(any(), anyByte(), any())).thenReturn(CompletableFuture.completedFuture(null));

    final EncryptDeviceCreationTimestampCommand encryptDeviceCreationTimestampCommand =
        new TestEncryptDeviceCreationTimestampCommand(accountsManager, isDryRun);

    try (MockedStatic<EncryptDeviceCreationTimestampUtil> mockUtil = mockStatic(EncryptDeviceCreationTimestampUtil.class)) {
      mockUtil.when(() -> EncryptDeviceCreationTimestampUtil.encrypt(anyLong(), any(), anyByte(), anyInt()))
          .thenReturn(TestRandomUtil.nextBytes(56));

      encryptDeviceCreationTimestampCommand.crawlAccounts(Flux.just(account));

      mockUtil.verify(() -> EncryptDeviceCreationTimestampUtil.encrypt(
          eq(createdAt),
          eq(identityKey),
          eq(deviceId),
          eq(registrationId)
      ));

      if (isDryRun) {
        verify(accountsManager, never()).updateDeviceAsync(any(), anyByte(), any());
      } else {
        verify(accountsManager).updateDeviceAsync(eq(account), eq(deviceId), any());
      }
    }
  }
}
