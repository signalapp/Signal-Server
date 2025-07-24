package org.whispersystems.textsecuregcm.workers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.push.IdleDeviceNotificationScheduler;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import reactor.core.publisher.Flux;

class NotifyIdleDevicesCommandTest {

  private MessagesManager messagesManager;
  private IdleDeviceNotificationScheduler idleDeviceNotificationScheduler;

  private TestNotifyIdleDevicesCommand notifyIdleDevicesWithoutMessagesCommand;

  private static final Instant CURRENT_TIME = Instant.now();

  private static class TestNotifyIdleDevicesCommand extends NotifyIdleDevicesCommand {

    private final CommandDependencies commandDependencies;
    private final IdleDeviceNotificationScheduler idleDeviceNotificationScheduler;

    private boolean dryRun = false;

    private TestNotifyIdleDevicesCommand(final MessagesManager messagesManager,
        final IdleDeviceNotificationScheduler idleDeviceNotificationScheduler) {

      this.commandDependencies = new CommandDependencies(
          null,
          null,
          null,
          null,
          messagesManager,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null,
          null);

      this.idleDeviceNotificationScheduler = idleDeviceNotificationScheduler;
    }

    public void setDryRun(final boolean dryRun) {
      this.dryRun = dryRun;
    }

    @Override
    protected CommandDependencies getCommandDependencies() {
      return commandDependencies;
    }

    @Override
    protected Clock getClock() {
      return Clock.fixed(CURRENT_TIME, ZoneId.systemDefault());
    }

    @Override
    protected IdleDeviceNotificationScheduler buildIdleDeviceNotificationScheduler() {
      return idleDeviceNotificationScheduler;
    }

    @Override
    protected Namespace getNamespace() {
      return new Namespace(Map.of(
          NotifyIdleDevicesCommand.MAX_CONCURRENCY_ARGUMENT, 1,
          NotifyIdleDevicesCommand.DRY_RUN_ARGUMENT, dryRun));
    }
  }

  @BeforeEach
  void setUp() {
    messagesManager = mock(MessagesManager.class);
    idleDeviceNotificationScheduler = mock(IdleDeviceNotificationScheduler.class);

    when(idleDeviceNotificationScheduler.scheduleNotification(any(), any(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    notifyIdleDevicesWithoutMessagesCommand =
        new TestNotifyIdleDevicesCommand(messagesManager, idleDeviceNotificationScheduler);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void crawlAccounts(final boolean dryRun) {
    notifyIdleDevicesWithoutMessagesCommand.setDryRun(dryRun);

    final UUID accountIdentifier = UUID.randomUUID();

    final Device eligibleDevice = mock(Device.class);
    when(eligibleDevice.getId()).thenReturn(Device.PRIMARY_ID);
    when(eligibleDevice.getApnId()).thenReturn("apns-token");
    when(eligibleDevice.getLastSeen())
        .thenReturn(CURRENT_TIME.minus(IdleWakeupEligibilityChecker.MIN_LONG_IDLE_DURATION).toEpochMilli());

    final Device ineligibleDevice = mock(Device.class);
    when(ineligibleDevice.getId()).thenReturn((byte) (Device.PRIMARY_ID + 1));


    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(account.getDevices()).thenReturn(List.of(eligibleDevice, ineligibleDevice));

    when(messagesManager.mayHavePersistedMessages(accountIdentifier, eligibleDevice))
        .thenReturn(CompletableFuture.completedFuture(false));

    notifyIdleDevicesWithoutMessagesCommand.crawlAccounts(Flux.just(account));

    if (dryRun) {
      verify(idleDeviceNotificationScheduler, never()).scheduleNotification(account, eligibleDevice, NotifyIdleDevicesCommand.PREFERRED_NOTIFICATION_TIME);
    } else {
      verify(idleDeviceNotificationScheduler).scheduleNotification(account, eligibleDevice, NotifyIdleDevicesCommand.PREFERRED_NOTIFICATION_TIME);
    }

    verify(idleDeviceNotificationScheduler, never()).scheduleNotification(eq(account), eq(ineligibleDevice), any());
  }
}
