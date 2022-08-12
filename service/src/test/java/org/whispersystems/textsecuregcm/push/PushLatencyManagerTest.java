/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicPushLatencyConfiguration;
import org.whispersystems.textsecuregcm.push.PushLatencyManager;
import org.whispersystems.textsecuregcm.push.PushLatencyManager.PushRecord;
import org.whispersystems.textsecuregcm.push.PushLatencyManager.PushType;
import org.whispersystems.textsecuregcm.redis.RedisClusterExtension;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

class PushLatencyManagerTest {

  @RegisterExtension
  static final RedisClusterExtension REDIS_CLUSTER_EXTENSION = RedisClusterExtension.builder().build();

  private DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  @BeforeEach
  void setUp() {
    //noinspection unchecked
    dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    final DynamicPushLatencyConfiguration dynamicPushLatencyConfiguration = mock(DynamicPushLatencyConfiguration.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getPushLatencyConfiguration()).thenReturn(dynamicPushLatencyConfiguration);
    when(dynamicPushLatencyConfiguration.getInstrumentedVersions()).thenReturn(Collections.emptyMap());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTakeRecord(final boolean isVoip) throws ExecutionException, InterruptedException {
    final UUID accountUuid = UUID.randomUUID();
    final long deviceId = 1;

    final Instant pushTimestamp = Instant.now();

    final PushLatencyManager pushLatencyManager = new PushLatencyManager(REDIS_CLUSTER_EXTENSION.getRedisCluster(),
        dynamicConfigurationManager, Clock.fixed(pushTimestamp, ZoneId.systemDefault()));

    assertNull(pushLatencyManager.takePushRecord(accountUuid, deviceId).get());

    pushLatencyManager.recordPushSent(accountUuid, deviceId, isVoip);

    final PushRecord pushRecord = pushLatencyManager.takePushRecord(accountUuid, deviceId).get();

    assertNotNull(pushRecord);
    assertEquals(pushTimestamp, pushRecord.getTimestamp());
    assertEquals(isVoip ? PushType.VOIP : PushType.STANDARD, pushRecord.getPushType());

    assertNull(pushLatencyManager.takePushRecord(accountUuid, deviceId).get());
  }
}
