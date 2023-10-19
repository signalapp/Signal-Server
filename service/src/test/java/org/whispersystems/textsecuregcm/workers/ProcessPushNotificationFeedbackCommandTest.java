/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import reactor.core.publisher.Flux;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProcessPushNotificationFeedbackCommandTest {

  private AccountsManager accountsManager;
  private Clock clock;

  private ProcessPushNotificationFeedbackCommand processPushNotificationFeedbackCommand;

  private static class TestProcessPushNotificationFeedbackCommand extends ProcessPushNotificationFeedbackCommand {

    private final CommandDependencies commandDependencies;
    private final Namespace namespace;

    public TestProcessPushNotificationFeedbackCommand(final Clock clock, final AccountsManager accountsManager, final boolean isDryRun) {
      super(clock);

      commandDependencies = mock(CommandDependencies.class);
      when(commandDependencies.accountsManager()).thenReturn(accountsManager);

      namespace = mock(Namespace.class);
      when(namespace.getBoolean(RemoveExpiredAccountsCommand.DRY_RUN_ARGUMENT)).thenReturn(isDryRun);
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

  @BeforeEach
  void setUpBeforeEach() {
    accountsManager = mock(AccountsManager.class);

    when(accountsManager.updateAsync(any(), any()))
        .thenAnswer(invocation -> {
          final Account account = invocation.getArgument(0);
          final Consumer<Account> accountUpdater = invocation.getArgument(1);

          accountUpdater.accept(account);

          return CompletableFuture.completedFuture(account);
        });

    clock = Clock.fixed(Instant.now(), ZoneId.systemDefault());

    processPushNotificationFeedbackCommand =
        new TestProcessPushNotificationFeedbackCommand(clock, accountsManager, true);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void crawlAccounts(final boolean isDryRun) {
    processPushNotificationFeedbackCommand =
        new TestProcessPushNotificationFeedbackCommand(clock, accountsManager, isDryRun);

    final Account accountWithActiveDevice = mock(Account.class);
    {
      final Device device = mock(Device.class);

      when(accountWithActiveDevice.getDevices()).thenReturn(List.of(device));
    }

    final Account accountWithUninstalledDevice = mock(Account.class);
    {
      final Device uninstalledDevice = mock(Device.class);
      when(uninstalledDevice.getUninstalledFeedbackTimestamp())
          .thenReturn(clock.instant().minus(ProcessPushNotificationFeedbackCommand.MAX_TOKEN_REFRESH_DELAY.multipliedBy(2)).toEpochMilli());

      when(accountWithUninstalledDevice.getDevices()).thenReturn(List.of(uninstalledDevice));
    }

    processPushNotificationFeedbackCommand.crawlAccounts(
        Flux.just(accountWithActiveDevice, accountWithUninstalledDevice).parallel());

    if (isDryRun) {
      verify(accountsManager, never()).updateAsync(any(), any());
    } else {
      verify(accountsManager, never()).updateAsync(eq(accountWithActiveDevice), any());
      verify(accountsManager).updateAsync(eq(accountWithUninstalledDevice), any());
    }
  }

  @Test
  void deviceNeedsUpdate() {
    {
      final Device deviceWithMaturePushNotificationFeedback = mock(Device.class);
      when(deviceWithMaturePushNotificationFeedback.getUninstalledFeedbackTimestamp())
          .thenReturn(clock.instant().minus(ProcessPushNotificationFeedbackCommand.MAX_TOKEN_REFRESH_DELAY.multipliedBy(2)).toEpochMilli());

      assertTrue(processPushNotificationFeedbackCommand.deviceNeedsUpdate(deviceWithMaturePushNotificationFeedback));
    }

    {
      final Device deviceWithRecentPushNotificationFeedback = mock(Device.class);
      when(deviceWithRecentPushNotificationFeedback.getUninstalledFeedbackTimestamp())
          .thenReturn(clock.instant().minus(ProcessPushNotificationFeedbackCommand.MAX_TOKEN_REFRESH_DELAY.dividedBy(2)).toEpochMilli());

      assertFalse(processPushNotificationFeedbackCommand.deviceNeedsUpdate(deviceWithRecentPushNotificationFeedback));
    }

    {
      final Device deviceWithoutPushNotificationFeedback = mock(Device.class);

      assertFalse(processPushNotificationFeedbackCommand.deviceNeedsUpdate(deviceWithoutPushNotificationFeedback));
    }
  }

  @Test
  void deviceExpired() {
    {
      final Device expiredDevice = mock(Device.class);
      when(expiredDevice.getLastSeen())
          .thenReturn(
              clock.instant().minus(ProcessPushNotificationFeedbackCommand.MAX_TOKEN_REFRESH_DELAY.multipliedBy(2))
                  .toEpochMilli());

      assertTrue(processPushNotificationFeedbackCommand.deviceExpired(expiredDevice));
    }

    {
      final Device activeDevice = mock(Device.class);
      when(activeDevice.getLastSeen()).thenReturn(clock.instant().toEpochMilli());

      assertFalse(processPushNotificationFeedbackCommand.deviceExpired(activeDevice));
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  @ParameterizedTest
  @MethodSource
  void getUserAgent(final Device device, final Optional<String> expectedUserAgentString) {
    assertEquals(expectedUserAgentString, ProcessPushNotificationFeedbackCommand.getUserAgent(device));
  }

  private static List<Arguments> getUserAgent() {
    final Device iosPrimaryDevice = mock(Device.class);
    when(iosPrimaryDevice.isPrimary()).thenReturn(true);
    when(iosPrimaryDevice.getApnId()).thenReturn("apns-token");

    final Device iosLinkedDevice = mock(Device.class);
    when(iosLinkedDevice.isPrimary()).thenReturn(false);
    when(iosLinkedDevice.getApnId()).thenReturn("apns-token");

    final Device androidDevice = mock(Device.class);
    when(androidDevice.getGcmId()).thenReturn("gcm-id");

    final Device deviceWithoutTokens = mock(Device.class);

    return List.of(
        Arguments.of(iosPrimaryDevice, Optional.of("OWI")),
        Arguments.of(iosLinkedDevice, Optional.of("OWP")),
        Arguments.of(androidDevice, Optional.of("OWA")),
        Arguments.of(deviceWithoutTokens, Optional.empty())
    );
  }
}
