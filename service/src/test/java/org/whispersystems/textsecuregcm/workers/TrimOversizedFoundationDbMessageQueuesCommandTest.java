/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class TrimOversizedFoundationDbMessageQueuesCommandTest {
  private final MessagesManager messagesManager = mock(MessagesManager.class);

  private static class TestTrimOversizedFoundationDbMessageQueuesCommand extends
      TrimOversizedFoundationDbMessageQueuesCommand {
    private final CommandDependencies commandDependencies;
    private final boolean dryRun;

    public TestTrimOversizedFoundationDbMessageQueuesCommand(
        final MessagesManager messagesManager,
        final boolean dryRun) {

      super();
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
          TrimOversizedFoundationDbMessageQueuesCommand.DRY_RUN_ARGUMENT, dryRun,
          TrimOversizedFoundationDbMessageQueuesCommand.MAX_CONCURRENCY_ARGUMENT, 4,
          TrimOversizedFoundationDbMessageQueuesCommand.MAX_QUEUE_SIZE_BYTES_ARGUMENT, TrimOversizedFoundationDbMessageQueuesCommand.DEFAULT_MAX_QUEUE_SIZE_BYTES,
          TrimOversizedFoundationDbMessageQueuesCommand.TARGET_QUEUE_SIZE_BYTES_ARGUMENT, TrimOversizedFoundationDbMessageQueuesCommand.DEFAULT_TARGET_QUEUE_SIZE_BYTES,
          TrimOversizedFoundationDbMessageQueuesCommand.RANGE_SPLIT_CHUNK_SIZE_BYTES_ARGUMENT, TrimOversizedFoundationDbMessageQueuesCommand.DEFAULT_RANGE_SPLIT_CHUNK_SIZE_BYTES
      ));
    }
  }

  @ParameterizedTest
  @ValueSource(booleans={true, false})
  void crawlAccounts(final boolean dryRun) {
    final TestTrimOversizedFoundationDbMessageQueuesCommand trimOversizedFoundationDbMessageQueuesCommand =
        new TestTrimOversizedFoundationDbMessageQueuesCommand(messagesManager, dryRun);

    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(Device.PRIMARY_ID);

    final UUID uuid = UUID.randomUUID();
    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(uuid);
    when(account.getDevices()).thenReturn(List.of(device));

    when(messagesManager.trimQueue(any(), any(), anyLong(), anyLong(), anyLong(), anyBoolean())).thenReturn(Mono.empty());

    trimOversizedFoundationDbMessageQueuesCommand.crawlAccounts(Flux.just(account));

    verify(messagesManager).trimQueue(
        eq(new AciServiceIdentifier(uuid)),
        eq(device),
        eq(TrimOversizedFoundationDbMessageQueuesCommand.DEFAULT_MAX_QUEUE_SIZE_BYTES),
        eq(TrimOversizedFoundationDbMessageQueuesCommand.DEFAULT_TARGET_QUEUE_SIZE_BYTES),
        eq(TrimOversizedFoundationDbMessageQueuesCommand.DEFAULT_RANGE_SPLIT_CHUNK_SIZE_BYTES),
        eq(dryRun)
    );
  }
}
