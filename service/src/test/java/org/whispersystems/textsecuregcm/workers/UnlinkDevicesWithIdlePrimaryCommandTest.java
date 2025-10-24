/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import net.sourceforge.argparse4j.inf.Namespace;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import reactor.core.publisher.Flux;

class UnlinkDevicesWithIdlePrimaryCommandTest {

  private static final Clock CLOCK = Clock.fixed(Instant.now(), ZoneId.systemDefault());

  private static class TestUnlinkDevicesWithIdlePrimaryCommand extends UnlinkDevicesWithIdlePrimaryCommand {

    private final CommandDependencies commandDependencies;
    private final Namespace namespace;

    public TestUnlinkDevicesWithIdlePrimaryCommand(final Clock clock,
        final AccountsManager accountsManager,
        final boolean isDryRun,
        final int enrollmentPercentage) {

      super(clock);

      commandDependencies = mock(CommandDependencies.class);
      when(commandDependencies.accountsManager()).thenReturn(accountsManager);

      namespace = new Namespace(Map.of(
          UnlinkDevicesWithIdlePrimaryCommand.DRY_RUN_ARGUMENT, isDryRun,
          UnlinkDevicesWithIdlePrimaryCommand.ENROLLMENT_PERCENTAGE_ARGUMENT, enrollmentPercentage,
          UnlinkDevicesWithIdlePrimaryCommand.MAX_CONCURRENCY_ARGUMENT, 16,
          UnlinkDevicesWithIdlePrimaryCommand.PRIMARY_IDLE_DAYS_ARGUMENT, UnlinkDevicesWithIdlePrimaryCommand.DEFAULT_PRIMARY_IDLE_DAYS
      ));
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
    final AccountsManager accountsManager = mock(AccountsManager.class);
    when(accountsManager.removeDevice(any(), anyByte()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final Duration idleDeviceLastSeenDuration =
        Duration.ofDays(UnlinkDevicesWithIdlePrimaryCommand.DEFAULT_PRIMARY_IDLE_DAYS).plus(Duration.ofDays(1));

    final Duration activeDeviceLastSeenDuration =
        Duration.ofDays(UnlinkDevicesWithIdlePrimaryCommand.DEFAULT_PRIMARY_IDLE_DAYS).minus(Duration.ofDays(1));

    final Account accountWithIdlePrimaryAndNoLinkedDevice = mock(Account.class);
    {
      when(accountWithIdlePrimaryAndNoLinkedDevice.getIdentifier(IdentityType.ACI)).thenReturn(UUID.randomUUID());

      final Device primaryDevice =
          generateMockDevice(Device.PRIMARY_ID, idleDeviceLastSeenDuration);

      when(accountWithIdlePrimaryAndNoLinkedDevice.getPrimaryDevice()).thenReturn(primaryDevice);
      when(accountWithIdlePrimaryAndNoLinkedDevice.getDevices()).thenReturn(List.of(primaryDevice));
    }

    final Account accountWithActivePrimaryAndLinkedDevice = mock(Account.class);
    {
      when(accountWithActivePrimaryAndLinkedDevice.getIdentifier(IdentityType.ACI)).thenReturn(UUID.randomUUID());

      final Device primaryDevice =
          generateMockDevice(Device.PRIMARY_ID, activeDeviceLastSeenDuration);

      final Device linkedDevice = generateMockDevice((byte) (Device.PRIMARY_ID + 1), activeDeviceLastSeenDuration);

      when(accountWithActivePrimaryAndLinkedDevice.getPrimaryDevice()).thenReturn(primaryDevice);
      when(accountWithActivePrimaryAndLinkedDevice.getDevices()).thenReturn(List.of(primaryDevice, linkedDevice));
    }

    final byte linkedDeviceId = Device.PRIMARY_ID + 2;

    final Account accountWithIdlePrimaryAndLinkedDevice = mock(Account.class);
    {
      when(accountWithIdlePrimaryAndLinkedDevice.getIdentifier(IdentityType.ACI)).thenReturn(UUID.randomUUID());

      final Device primaryDevice =
          generateMockDevice(Device.PRIMARY_ID, idleDeviceLastSeenDuration);

      final Device linkedDevice = generateMockDevice(linkedDeviceId, activeDeviceLastSeenDuration);

      when(accountWithIdlePrimaryAndLinkedDevice.getPrimaryDevice()).thenReturn(primaryDevice);
      when(accountWithIdlePrimaryAndLinkedDevice.getDevices()).thenReturn(List.of(primaryDevice, linkedDevice));
    }

    final UnlinkDevicesWithIdlePrimaryCommand unlinkDevicesWithIdlePrimaryCommand =
        new TestUnlinkDevicesWithIdlePrimaryCommand(CLOCK, accountsManager, isDryRun, 100);

    unlinkDevicesWithIdlePrimaryCommand.crawlAccounts(Flux.just(accountWithIdlePrimaryAndNoLinkedDevice,
        accountWithActivePrimaryAndLinkedDevice,
        accountWithIdlePrimaryAndLinkedDevice));

    if (!isDryRun) {
      verify(accountsManager).removeDevice(accountWithIdlePrimaryAndLinkedDevice, linkedDeviceId);
    }

    verifyNoMoreInteractions(accountsManager);
  }

  @Test
  void crawlAccountsPartialEnrollment() {
    final AccountsManager accountsManager = mock(AccountsManager.class);
    when(accountsManager.removeDevice(any(), anyByte()))
        .thenReturn(CompletableFuture.completedFuture(null));

    final UUID enrolledAccountIdentifier = randomUUIDWithEnrollmentHash(1);
    final UUID unenrolledAccountIdentifier = randomUUIDWithEnrollmentHash(25);

    final byte linkedDeviceId = Device.PRIMARY_ID + 1;

    final Duration idleDeviceLastSeenDuration =
        Duration.ofDays(UnlinkDevicesWithIdlePrimaryCommand.DEFAULT_PRIMARY_IDLE_DAYS).plus(Duration.ofDays(1));

    final Account enrolledAccount = mock(Account.class);
    {
      when(enrolledAccount.getIdentifier(IdentityType.ACI)).thenReturn(enrolledAccountIdentifier);

      final Device primaryDevice =
          generateMockDevice(Device.PRIMARY_ID, idleDeviceLastSeenDuration);

      final Device linkedDevice = generateMockDevice(linkedDeviceId, idleDeviceLastSeenDuration);

      when(enrolledAccount.getPrimaryDevice()).thenReturn(primaryDevice);
      when(enrolledAccount.getDevices()).thenReturn(List.of(primaryDevice, linkedDevice));
    }

    final Account unenrolledAccount = mock(Account.class);
    {
      when(unenrolledAccount.getIdentifier(IdentityType.ACI)).thenReturn(unenrolledAccountIdentifier);

      final Device primaryDevice =
          generateMockDevice(Device.PRIMARY_ID, idleDeviceLastSeenDuration);

      final Device linkedDevice = generateMockDevice(linkedDeviceId, idleDeviceLastSeenDuration);

      when(unenrolledAccount.getPrimaryDevice()).thenReturn(primaryDevice);
      when(unenrolledAccount.getDevices()).thenReturn(List.of(primaryDevice, linkedDevice));
    }

    final UnlinkDevicesWithIdlePrimaryCommand unlinkDevicesWithIdlePrimaryCommand =
        new TestUnlinkDevicesWithIdlePrimaryCommand(CLOCK, accountsManager, false, 10);

    unlinkDevicesWithIdlePrimaryCommand.crawlAccounts(Flux.just(enrolledAccount, unenrolledAccount));

    verify(accountsManager).removeDevice(enrolledAccount, linkedDeviceId);
    verifyNoMoreInteractions(accountsManager);
  }

  private static UUID randomUUIDWithEnrollmentHash(final int enrollmentHash) {
    UUID uuid;

    do {
      uuid = UUID.randomUUID();
    } while ((uuid.hashCode() & Integer.MAX_VALUE) % 100 != enrollmentHash);

    return uuid;
  }

  private static Device generateMockDevice(final byte deviceId, final Duration primaryIdleDuration) {
    final Device device = mock(Device.class);
    when(device.getId()).thenReturn(deviceId);
    when(device.isPrimary()).thenReturn(deviceId == Device.PRIMARY_ID);
    when(device.getLastSeen()).thenReturn(CLOCK.instant().minus(primaryIdleDuration).toEpochMilli());

    return device;
  }
}
