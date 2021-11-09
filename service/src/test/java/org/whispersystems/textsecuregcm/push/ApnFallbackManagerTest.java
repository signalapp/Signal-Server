/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.lettuce.core.cluster.SlotHash;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.mockito.ArgumentCaptor;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Pair;

class ApnFallbackManagerTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private Account account;
  private Device device;

  private APNSender apnSender;

  private ApnFallbackManager apnFallbackManager;

  private static final UUID ACCOUNT_UUID = UUID.randomUUID();
  private static final String ACCOUNT_NUMBER = "+18005551234";
  private static final long DEVICE_ID = 1L;
  private static final String VOIP_APN_ID = RandomStringUtils.randomAlphanumeric(32);

  @BeforeEach
  void setUp() throws Exception {

    device = mock(Device.class);
    when(device.getId()).thenReturn(DEVICE_ID);
    when(device.getVoipApnId()).thenReturn(VOIP_APN_ID);
    when(device.getLastSeen()).thenReturn(System.currentTimeMillis());

    account = mock(Account.class);
    when(account.getUuid()).thenReturn(ACCOUNT_UUID);
    when(account.getNumber()).thenReturn(ACCOUNT_NUMBER);
    when(account.getDevice(DEVICE_ID)).thenReturn(Optional.of(device));

    final AccountsManager accountsManager = mock(AccountsManager.class);
    when(accountsManager.getByE164(ACCOUNT_NUMBER)).thenReturn(Optional.of(account));
    when(accountsManager.getByAccountIdentifier(ACCOUNT_UUID)).thenReturn(Optional.of(account));

    apnSender = mock(APNSender.class);

    apnFallbackManager = new ApnFallbackManager(REDIS_CLUSTER_EXTENSION.getRedisCluster(), apnSender, accountsManager);
  }

  @Test
  void testClusterInsert() {
    final String endpoint = apnFallbackManager.getEndpointKey(account, device);

    assertTrue(apnFallbackManager.getPendingDestinations(SlotHash.getSlot(endpoint), 1).isEmpty());

    apnFallbackManager.schedule(account, device, System.currentTimeMillis() - 30_000);

    final List<String> pendingDestinations = apnFallbackManager.getPendingDestinations(SlotHash.getSlot(endpoint), 2);
    assertEquals(1, pendingDestinations.size());

    final Optional<Pair<String, Long>> maybeUuidAndDeviceId = ApnFallbackManager.getSeparated(
        pendingDestinations.get(0));

    assertTrue(maybeUuidAndDeviceId.isPresent());
    assertEquals(ACCOUNT_UUID.toString(), maybeUuidAndDeviceId.get().first());
    assertEquals(DEVICE_ID, (long) maybeUuidAndDeviceId.get().second());

    assertTrue(apnFallbackManager.getPendingDestinations(SlotHash.getSlot(endpoint), 1).isEmpty());
  }

  @Test
  void testProcessNextSlot() {
    final ApnFallbackManager.NotificationWorker worker = apnFallbackManager.new NotificationWorker();

    apnFallbackManager.schedule(account, device, System.currentTimeMillis() - 30_000);

    final int slot = SlotHash.getSlot(apnFallbackManager.getEndpointKey(account, device));
    final int previousSlot = (slot + SlotHash.SLOT_COUNT - 1) % SlotHash.SLOT_COUNT;

    REDIS_CLUSTER_EXTENSION.getRedisCluster().withCluster(connection -> connection.sync()
        .set(ApnFallbackManager.NEXT_SLOT_TO_PERSIST_KEY, String.valueOf(previousSlot)));

    assertEquals(1, worker.processNextSlot());

    final ArgumentCaptor<ApnMessage> messageCaptor = ArgumentCaptor.forClass(ApnMessage.class);
    verify(apnSender).sendMessage(messageCaptor.capture());

    final ApnMessage message = messageCaptor.getValue();

    assertEquals(VOIP_APN_ID, message.getApnId());
    assertEquals(Optional.of(ACCOUNT_UUID), message.getUuid());
    assertEquals(DEVICE_ID, message.getDeviceId());

    assertEquals(0, worker.processNextSlot());
  }
}
