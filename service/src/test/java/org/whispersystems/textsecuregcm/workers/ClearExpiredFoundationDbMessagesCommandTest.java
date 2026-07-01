/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import org.whispersystems.textsecuregcm.util.TestClock;
import reactor.core.publisher.Flux;

class ClearExpiredFoundationDbMessagesCommandTest {

  private MessagesManager messagesManager = mock(MessagesManager.class);

  private static class TestClearExpiredFoundationDbMessagesCommand extends ClearExpiredFoundationDbMessagesCommand {

    private final CommandDependencies commandDependencies;

    private final boolean dryRun;

    public TestClearExpiredFoundationDbMessagesCommand(final MessagesManager messagesManager,
        final boolean dryRun,
        final Clock clock) {

      super(clock);

      this.commandDependencies = new CommandDependencies(null, null, null, null, messagesManager, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
      this.dryRun = dryRun;
    }

    @Override
    protected CommandDependencies getCommandDependencies() {
      return commandDependencies;
    }

    @Override
    protected Namespace getNamespace() {
      return new Namespace(Map.of(
          ClearExpiredFoundationDbMessagesCommand.BATCH_SIZE_ARGUMENT, 4,
          ClearExpiredFoundationDbMessagesCommand.DRY_RUN_ARGUMENT, dryRun,
          ClearExpiredFoundationDbMessagesCommand.MESSAGE_TTL_DAYS_ARGUMENT, 1,
          ClearExpiredFoundationDbMessagesCommand.VERSIONSTAMP_TTL_DAYS_ARGUMENT, 10
      ));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans={true, false})
  void crawlAccounts(final boolean dryRun) {

    final Clock clock = TestClock.pinned(Instant.now());
    final Instant messageCutoffTime = clock.instant().minus(Duration.ofDays(1));
    final Instant versionstampCutoffTime = clock.instant().minus(Duration.ofDays(10));

    final TestClearExpiredFoundationDbMessagesCommand clearExpiredFoundationDbMessagesCommand =
        new TestClearExpiredFoundationDbMessagesCommand(messagesManager,
            dryRun,
            clock);

    final List<Account> accounts = new ArrayList<>();
    final AciServiceIdentifier[] acis = new AciServiceIdentifier[10];
    for (int i = 0; i < 10; i++) {
      final UUID accountIdentifier = UUID.randomUUID();
      final byte primaryDeviceId = Device.PRIMARY_ID;
      final byte linkedDeviceId = 2;

      final Device primaryDevice = mock(Device.class);
      when(primaryDevice.getId()).thenReturn(primaryDeviceId);

      final Device linkedDevice = mock(Device.class);
      when(linkedDevice.getId()).thenReturn(linkedDeviceId);

      final Account account = mock(Account.class);
      when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
      when(account.getDevices()).thenReturn(i % 2 == 0 ? List.of(primaryDevice) : List.of(primaryDevice, linkedDevice));

      accounts.add(account);
      acis[i] = new AciServiceIdentifier(accountIdentifier);
    }

    clearExpiredFoundationDbMessagesCommand.crawlAccounts(Flux.fromIterable(accounts));

    final List<Byte> oneDeviceId = List.of(Device.PRIMARY_ID);
    final List<Byte> twoDeviceIds = List.of(Device.PRIMARY_ID, (byte) 2);
    if (!dryRun) {
      verify(messagesManager).deleteFoundationDbMessagesBefore(
          Map.of(acis[0], oneDeviceId, acis[1], twoDeviceIds, acis[2], oneDeviceId, acis[3], twoDeviceIds),
          messageCutoffTime);
      verify(messagesManager).deleteFoundationDbMessagesBefore(
          Map.of(acis[4], oneDeviceId, acis[5], twoDeviceIds, acis[6], oneDeviceId, acis[7], twoDeviceIds),
          messageCutoffTime);
      verify(messagesManager).deleteFoundationDbMessagesBefore(
          Map.of(acis[8], oneDeviceId, acis[9], twoDeviceIds),
          messageCutoffTime);
    } else {
      verify(messagesManager, never()).deleteFoundationDbMessagesBefore(any(), any());
    }

    verify(messagesManager).recordFoundationDbVersionstamps();
    verify(messagesManager)
        .expireOldFoundationDbVersionstamps(versionstampCutoffTime);
  }
}
