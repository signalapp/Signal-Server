package org.whispersystems.textsecuregcm.workers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.i18n.phonenumbers.PhoneNumberUtil;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
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
        .thenReturn(CURRENT_TIME.minus(NotifyIdleDevicesCommand.MIN_LONG_IDLE_DURATION).toEpochMilli());

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

  @ParameterizedTest
  @MethodSource
  void isDeviceEligible(final Account account,
      final Device device,
      final boolean mayHaveMessages,
      final boolean mayHaveUrgentMessages,
      final boolean expectEligible) {

    when(messagesManager.mayHavePersistedMessages(account.getIdentifier(IdentityType.ACI), device))
        .thenReturn(CompletableFuture.completedFuture(mayHaveMessages));

    when(messagesManager.mayHaveUrgentPersistedMessages(account.getIdentifier(IdentityType.ACI), device))
        .thenReturn(CompletableFuture.completedFuture(mayHaveUrgentMessages));

    assertEquals(expectEligible,
        NotifyIdleDevicesCommand.isDeviceEligible(account, device, messagesManager, Clock.fixed(CURRENT_TIME, ZoneId.systemDefault())).block());
  }

  private static List<Arguments> isDeviceEligible() {
    final List<Arguments> arguments = new ArrayList<>();

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(UUID.randomUUID());
    when(account.getNumber()).thenReturn(PhoneNumberUtil.getInstance().format(
        PhoneNumberUtil.getInstance().getExampleNumber("US"), PhoneNumberUtil.PhoneNumberFormat.E164));

    {
      // Long-idle device with push token and messages
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.minus(NotifyIdleDevicesCommand.MIN_LONG_IDLE_DURATION).toEpochMilli());

      arguments.add(Arguments.of(account, device, true, true, false));
    }

    {
      // Long-idle device missing push token, but with messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.minus(NotifyIdleDevicesCommand.MIN_LONG_IDLE_DURATION).toEpochMilli());

      arguments.add(Arguments.of(account, device, true, true, false));
    }

    {
      // Long-idle device missing push token and messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.minus(NotifyIdleDevicesCommand.MIN_LONG_IDLE_DURATION).toEpochMilli());

      arguments.add(Arguments.of(account, device, false, false, false));
    }

    {
      // Long-idle device with push token, but no messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.minus(NotifyIdleDevicesCommand.MIN_LONG_IDLE_DURATION).toEpochMilli());
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(account, device, false, false, true));
    }

    {
      // Short-idle device with push token and urgent messages
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.minus(NotifyIdleDevicesCommand.MIN_SHORT_IDLE_DURATION).toEpochMilli());

      arguments.add(Arguments.of(account, device, true, true, true));
    }

    {
      // Short-idle device with push token and only non-urgent messages
      final Device device = mock(Device.class);
      when(device.getApnId()).thenReturn("apns-token");
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.minus(NotifyIdleDevicesCommand.MIN_SHORT_IDLE_DURATION).toEpochMilli());

      arguments.add(Arguments.of(account, device, true, false, false));
    }

    {
      // Short-idle device missing push token, but with urgent messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.minus(NotifyIdleDevicesCommand.MIN_SHORT_IDLE_DURATION).toEpochMilli());

      arguments.add(Arguments.of(account, device, true, true, false));
    }

    {
      // Short-idle device missing push token and messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.minus(NotifyIdleDevicesCommand.MIN_SHORT_IDLE_DURATION).toEpochMilli());

      arguments.add(Arguments.of(account, device, false, false, false));
    }

    {
      // Short-idle device with push token, but no messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.minus(NotifyIdleDevicesCommand.MIN_SHORT_IDLE_DURATION).toEpochMilli());
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(account, device, false, false, false));
    }

    {
      // Active device with push token and urgent messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.toEpochMilli());
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(account, device, true, true, false));
    }

    {
      // Active device missing push token, but with urgent messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.toEpochMilli());

      arguments.add(Arguments.of(account, device, true, true, false));
    }

    {
      // Active device missing push token and messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.toEpochMilli());

      arguments.add(Arguments.of(account, device, false, false, false));
    }

    {
      // Active device with push token, but no messages
      final Device device = mock(Device.class);
      when(device.getLastSeen()).thenReturn(CURRENT_TIME.toEpochMilli());
      when(device.getApnId()).thenReturn("apns-token");

      arguments.add(Arguments.of(account, device, false, false, false));
    }

    return arguments;
  }

  @ParameterizedTest
  @MethodSource
  void isShortIdle(final Duration idleDuration, final boolean expectIdle) {
    final Instant currentTime = Instant.now();
    final Clock clock = Clock.fixed(currentTime, ZoneId.systemDefault());

    final Device device = mock(Device.class);
    when(device.getLastSeen()).thenReturn(currentTime.minus(idleDuration).toEpochMilli());

    assertEquals(expectIdle, NotifyIdleDevicesCommand.isShortIdle(device, clock));
  }

  private static List<Arguments> isShortIdle() {
    return List.of(
        Arguments.of(NotifyIdleDevicesCommand.MIN_SHORT_IDLE_DURATION, true),
        Arguments.of(NotifyIdleDevicesCommand.MIN_SHORT_IDLE_DURATION.plusMillis(1), true),
        Arguments.of(NotifyIdleDevicesCommand.MIN_SHORT_IDLE_DURATION.minusMillis(1), false),
        Arguments.of(NotifyIdleDevicesCommand.MAX_SHORT_IDLE_DURATION, false),
        Arguments.of(NotifyIdleDevicesCommand.MAX_SHORT_IDLE_DURATION.plusMillis(1), false),
        Arguments.of(NotifyIdleDevicesCommand.MAX_SHORT_IDLE_DURATION.minusMillis(1), true)
    );
  }

  @ParameterizedTest
  @MethodSource
  void isLongIdle(final Duration idleDuration, final boolean expectIdle) {
    final Instant currentTime = Instant.now();
    final Clock clock = Clock.fixed(currentTime, ZoneId.systemDefault());

    final Device device = mock(Device.class);
    when(device.getLastSeen()).thenReturn(currentTime.minus(idleDuration).toEpochMilli());

    assertEquals(expectIdle, NotifyIdleDevicesCommand.isLongIdle(device, clock));
  }

  private static List<Arguments> isLongIdle() {
    return List.of(
        Arguments.of(NotifyIdleDevicesCommand.MIN_LONG_IDLE_DURATION, true),
        Arguments.of(NotifyIdleDevicesCommand.MIN_LONG_IDLE_DURATION.plusMillis(1), true),
        Arguments.of(NotifyIdleDevicesCommand.MIN_LONG_IDLE_DURATION.minusMillis(1), false),
        Arguments.of(NotifyIdleDevicesCommand.MAX_LONG_IDLE_DURATION, false),
        Arguments.of(NotifyIdleDevicesCommand.MAX_LONG_IDLE_DURATION.plusMillis(1), false),
        Arguments.of(NotifyIdleDevicesCommand.MAX_LONG_IDLE_DURATION.minusMillis(1), true)
    );
  }

  @ParameterizedTest
  @MethodSource
  void hasPushToken(final Device device, final boolean expectHasPushToken) {
    assertEquals(expectHasPushToken, NotifyIdleDevicesCommand.hasPushToken(device));
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

    return arguments;
  }
}
