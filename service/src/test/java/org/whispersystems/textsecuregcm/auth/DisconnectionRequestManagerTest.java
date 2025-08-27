/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;
import org.whispersystems.textsecuregcm.identity.IdentityType;
import org.whispersystems.textsecuregcm.redis.RedisServerExtension;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.Device;

@Timeout(value = 5, threadMode = Timeout.ThreadMode.SEPARATE_THREAD)
class DisconnectionRequestManagerTest {

  private GrpcClientConnectionManager grpcClientConnectionManager;
  private DisconnectionRequestManager disconnectionRequestManager;

  @RegisterExtension
  static final RedisServerExtension REDIS_EXTENSION = RedisServerExtension.builder().build();

  @BeforeEach
  void setUp() {
    grpcClientConnectionManager = mock(GrpcClientConnectionManager.class);

    disconnectionRequestManager = new DisconnectionRequestManager(REDIS_EXTENSION.getRedisClient(),
        grpcClientConnectionManager,
        Runnable::run,
        mock(ScheduledExecutorService.class));

    disconnectionRequestManager.start();
  }

  @AfterEach
  void tearDown() {
    disconnectionRequestManager.stop();
  }

  @Test
  void addRemoveListener() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte deviceId = Device.PRIMARY_ID;

    final DisconnectionRequestListener firstListener = mock(DisconnectionRequestListener.class);
    final DisconnectionRequestListener secondListener = mock(DisconnectionRequestListener.class);

    assertTrue(disconnectionRequestManager.getListeners(accountIdentifier, deviceId).isEmpty());

    disconnectionRequestManager.addListener(accountIdentifier, deviceId, firstListener);

    assertEquals(List.of(firstListener), disconnectionRequestManager.getListeners(accountIdentifier, deviceId));

    disconnectionRequestManager.addListener(accountIdentifier, deviceId, secondListener);

    assertEquals(List.of(firstListener, secondListener),
        disconnectionRequestManager.getListeners(accountIdentifier, deviceId));

    disconnectionRequestManager.removeListener(accountIdentifier, deviceId, mock(DisconnectionRequestListener.class));

    assertEquals(List.of(firstListener, secondListener),
        disconnectionRequestManager.getListeners(accountIdentifier, deviceId));

    disconnectionRequestManager.removeListener(accountIdentifier, deviceId, firstListener);

    assertEquals(List.of(secondListener), disconnectionRequestManager.getListeners(accountIdentifier, deviceId));
  }

  @Test
  void requestDisconnection() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte primaryDeviceId = Device.PRIMARY_ID;
    final byte linkedDeviceId = primaryDeviceId + 1;

    final UUID otherAccountIdentifier = UUID.randomUUID();
    final byte otherDeviceId = linkedDeviceId + 1;

    final List<Byte> deviceIds = List.of(primaryDeviceId, linkedDeviceId);

    final DisconnectionRequestListener primaryDeviceListener = mock(DisconnectionRequestListener.class);
    final DisconnectionRequestListener linkedDeviceListener = mock(DisconnectionRequestListener.class);

    disconnectionRequestManager.addListener(accountIdentifier, primaryDeviceId, primaryDeviceListener);
    disconnectionRequestManager.addListener(accountIdentifier, linkedDeviceId, linkedDeviceListener);

    disconnectionRequestManager.requestDisconnection(accountIdentifier, deviceIds).toCompletableFuture().join();

    verify(primaryDeviceListener, timeout(1_000)).handleDisconnectionRequest();
    verify(linkedDeviceListener, timeout(1_000)).handleDisconnectionRequest();
    verify(grpcClientConnectionManager, timeout(1_000))
        .closeConnection(new AuthenticatedDevice(accountIdentifier, primaryDeviceId));

    verify(grpcClientConnectionManager, timeout(1_000))
        .closeConnection(new AuthenticatedDevice(accountIdentifier, linkedDeviceId));

    disconnectionRequestManager.requestDisconnection(otherAccountIdentifier, List.of(otherDeviceId));

    verify(grpcClientConnectionManager, timeout(1_000))
        .closeConnection(new AuthenticatedDevice(otherAccountIdentifier, otherDeviceId));
  }

  @Test
  void requestDisconnectionAllDevices() {
    final UUID accountIdentifier = UUID.randomUUID();
    final byte primaryDeviceId = Device.PRIMARY_ID;
    final byte linkedDeviceId = primaryDeviceId + 1;

    final Device primaryDevice = mock(Device.class);
    when(primaryDevice.getId()).thenReturn(primaryDeviceId);

    final Device linkedDevice = mock(Device.class);
    when(linkedDevice.getId()).thenReturn(linkedDeviceId);

    final Account account = mock(Account.class);
    when(account.getIdentifier(IdentityType.ACI)).thenReturn(accountIdentifier);
    when(account.getDevices()).thenReturn(List.of(primaryDevice, linkedDevice));

    final DisconnectionRequestListener primaryDeviceListener = mock(DisconnectionRequestListener.class);
    final DisconnectionRequestListener linkedDeviceListener = mock(DisconnectionRequestListener.class);

    disconnectionRequestManager.addListener(accountIdentifier, primaryDeviceId, primaryDeviceListener);
    disconnectionRequestManager.addListener(accountIdentifier, linkedDeviceId, linkedDeviceListener);

    disconnectionRequestManager.requestDisconnection(account).toCompletableFuture().join();

    verify(primaryDeviceListener, timeout(1_000)).handleDisconnectionRequest();
    verify(linkedDeviceListener, timeout(1_000)).handleDisconnectionRequest();

    verify(grpcClientConnectionManager, timeout(1_000))
        .closeConnection(new AuthenticatedDevice(accountIdentifier, primaryDeviceId));

    verify(grpcClientConnectionManager, timeout(1_000))
        .closeConnection(new AuthenticatedDevice(accountIdentifier, linkedDeviceId));
  }
}
