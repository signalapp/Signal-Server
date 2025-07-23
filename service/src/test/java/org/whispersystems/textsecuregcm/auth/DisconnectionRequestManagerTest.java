/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.redis.RedisServerExtension;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

@Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DisconnectionRequestManagerTest {

  private DisconnectionRequestManager disconnectionRequestManager;

  @RegisterExtension
  static final RedisServerExtension REDIS_EXTENSION = RedisServerExtension.builder().build();

  private static class DisconnectionRequestTestListener implements DisconnectionRequestListener {

    private final CountDownLatch requestLatch = new CountDownLatch(1);

    private UUID accountIdentifier;
    private Collection<Byte> deviceIds;

    @Override
    public void handleDisconnectionRequest(final UUID accountIdentifier, final Collection<Byte> deviceIds) {
      this.accountIdentifier = accountIdentifier;
      this.deviceIds = deviceIds;

      requestLatch.countDown();
    }

    public UUID getAccountIdentifier() {
      return accountIdentifier;
    }

    public Collection<Byte> getDeviceIds() {
      return deviceIds;
    }

    public void waitForRequest() throws InterruptedException {
      requestLatch.await();
    }
  }

  @BeforeEach
  void setUp() {
    disconnectionRequestManager = new DisconnectionRequestManager(REDIS_EXTENSION.getRedisClient(), Runnable::run);
    disconnectionRequestManager.start();
  }

  @AfterEach
  void tearDown() {
    disconnectionRequestManager.stop();
  }

  @Test
  void requestDisconnection() throws InterruptedException {
    final UUID accountIdentifier = UUID.randomUUID();
    final List<Byte> deviceIds = List.of(Device.PRIMARY_ID, (byte) (Device.PRIMARY_ID + 1));

    final DisconnectionRequestTestListener listener = new DisconnectionRequestTestListener();

    disconnectionRequestManager.addListener(listener);
    disconnectionRequestManager.requestDisconnection(accountIdentifier, deviceIds).toCompletableFuture().join();

    listener.waitForRequest();

    assertEquals(accountIdentifier, listener.getAccountIdentifier());
    assertEquals(deviceIds, listener.getDeviceIds());
  }

  @Test
  void requestDisconnectionAllDevices() throws InterruptedException {
    final Device primaryDevice = mock(Device.class);
    when(primaryDevice.getId()).thenReturn(Device.PRIMARY_ID);

    final Device linkedDevice = mock(Device.class);
    when(linkedDevice.getId()).thenReturn((byte) (Device.PRIMARY_ID + 1));

    final UUID accountIdentifier = UUID.randomUUID();

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(account.getDevices()).thenReturn(List.of(primaryDevice, linkedDevice));

    final DisconnectionRequestTestListener listener = new DisconnectionRequestTestListener();

    disconnectionRequestManager.addListener(listener);
    disconnectionRequestManager.requestDisconnection(account).toCompletableFuture().join();

    listener.waitForRequest();

    assertEquals(accountIdentifier, listener.getAccountIdentifier());
    assertEquals(List.of(Device.PRIMARY_ID, (byte) (Device.PRIMARY_ID + 1)), listener.getDeviceIds());
  }
}
