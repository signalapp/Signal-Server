package org.whispersystems.textsecuregcm.workers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.push.IdleDeviceNotificationScheduler;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.MessagesManager;
import reactor.core.publisher.Flux;

class NotifyIdleDevicesWithoutMessagesCommandTest {

  private MessagesManager messagesManager;
  private IdleDeviceNotificationScheduler idleDeviceNotificationScheduler;

  private TestNotifyIdleDevicesWithoutMessagesCommand notifyIdleDevicesWithoutMessagesCommand;

  private static class TestNotifyIdleDevicesWithoutMessagesCommand extends NotifyIdleDevicesWithoutMessagesCommand {

    private final CommandDependencies commandDependencies;
    private final IdleDeviceNotificationScheduler idleDeviceNotificationScheduler;
    private boolean dryRun = false;

    private TestNotifyIdleDevicesWithoutMessagesCommand(final MessagesManager messagesManager,
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
    protected IdleDeviceNotificationScheduler buildIdleDeviceNotificationScheduler() {
      return idleDeviceNotificationScheduler;
    }

    @Override
    protected Namespace getNamespace() {
      return new Namespace(Map.of(
          NotifyIdleDevicesWithoutMessagesCommand.MAX_CONCURRENCY_ARGUMENT, 1,
          NotifyIdleDevicesWithoutMessagesCommand.DRY_RUN_ARGUMENT, dryRun));
    }
  }

  @BeforeEach
  void setUp() {
    messagesManager = mock(MessagesManager.class);
    idleDeviceNotificationScheduler = mock(IdleDeviceNotificationScheduler.class);

    when(idleDeviceNotificationScheduler.scheduleNotification(any(), anyByte(), any()))
        .thenReturn(CompletableFuture.completedFuture(null));

    notifyIdleDevicesWithoutMessagesCommand =
        new TestNotifyIdleDevicesWithoutMessagesCommand(messagesManager, idleDeviceNotificationScheduler);
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void crawlAccounts(final boolean dryRun) {
    notifyIdleDevicesWithoutMessagesCommand.setDryRun(dryRun);

    final UUID accountIdentifier = UUID.randomUUID();
    final byte eligibleDeviceId = Device.PRIMARY_ID;
    final byte ineligibleDeviceId = eligibleDeviceId + 1;

    final Device eligibleDevice = mock(Device.class);
    when(eligibleDevice.getId()).thenReturn(eligibleDeviceId);
    when(eligibleDevice.getApnId()).thenReturn("apns-token");

    final Device ineligibleDevice = mock(Device.class);
    when(ineligibleDevice.getId()).thenReturn(ineligibleDeviceId);


    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(account.getDevices()).thenReturn(List.of(eligibleDevice, ineligibleDevice));

    when(idleDeviceNotificationScheduler.isIdle(eligibleDevice)).thenReturn(true);
    when(messagesManager.mayHavePersistedMessages(accountIdentifier, eligibleDevice))
        .thenReturn(CompletableFuture.completedFuture(false));

    notifyIdleDevicesWithoutMessagesCommand.crawlAccounts(Flux.just(account));

    if (dryRun) {
      verify(idleDeviceNotificationScheduler, never()).scheduleNotification(account, eligibleDeviceId, NotifyIdleDevicesWithoutMessagesCommand.PREFERRED_NOTIFICATION_TIME);
    } else {
      verify(idleDeviceNotificationScheduler).scheduleNotification(account, eligibleDeviceId, NotifyIdleDevicesWithoutMessagesCommand.PREFERRED_NOTIFICATION_TIME);
    }

    verify(idleDeviceNotificationScheduler, never()).scheduleNotification(eq(account), eq(ineligibleDeviceId), any());
  }

  @ParameterizedTest
  @MethodSource
  void isDeviceEligible(final Account account,
      final Device device,
      final boolean isDeviceIdle,
      final boolean mayHaveMessages,
      final boolean expectEligible) {

    when(messagesManager.mayHavePersistedMessages(account.getIdentifier(IdentityType.ACI), device))
        .thenReturn(CompletableFuture.completedFuture(mayHaveMessages));

    when(idleDeviceNotificationScheduler.isIdle(device)).thenReturn(isDeviceIdle);

    assertEquals(expectEligible, NotifyIdleDevicesWithoutMessagesCommand.isDeviceEligible(account, device, idleDeviceNotificationScheduler, messagesManager).block());
  }

  private static List<Arguments> isDeviceEligible() {
    final List<Arguments> arguments = new ArrayList<>();

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(UUID.randomUUID());
    when(account.getNumber()).thenReturn(PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164));

    {
      // Idle device with push token and messages
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(account, device, true, true, false));
    }

    {
      // Idle device missing push token, but with messages
      arguments.add(Arguments.of(account, mock(Device.class), true, true, false));
    }

    {
      // Idle device missing push token and messages
      arguments.add(Arguments.of(account, mock(Device.class), true, false, false));
    }

    {
      // Idle device with push token, but no messages
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(account, device, true, false, true));
    }

    {
      // Active device with push token and messages
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(account, device, false, true, false));
    }

    {
      // Active device missing push token, but with messages
      arguments.add(Arguments.of(account, mock(Device.class), false, true, false));
    }

    {
      // Active device missing push token and messages
      arguments.add(Arguments.of(account, mock(Device.class), false, false, false));
    }

    {
      // Active device with push token, but no messages
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(account, device, false, false, false));
    }

    return arguments;
  }

  @ParameterizedTest
  @MethodSource
  void hasPushToken(final Device device, final boolean expectHasPushToken) {
    assertEquals(expectHasPushToken, NotifyIdleDevicesWithoutMessagesCommand.hasPushToken(device));
  }

  private static List<Arguments> hasPushToken() {
    final List<Arguments> arguments = new ArrayList<>();

    {
      // No token at all
      final Device device = mock(Device.class);

      arguments.add(Arguments.of(device, false));
    }

    {
      // FCM token
      final Device device = mock(Device.class);
      when(device.getGcmId()).thenReturn("fcm-token");

      arguments.add(Arguments.of(device, true));
    }

    {
      // APNs token
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(device, true));
    }

    {
      // APNs VOIP token
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");
      when(device.getVoipApnId()).thenReturn("apns-voip-token");

      arguments.add(Arguments.of(device, false));
    }

    return arguments;
  }
}
