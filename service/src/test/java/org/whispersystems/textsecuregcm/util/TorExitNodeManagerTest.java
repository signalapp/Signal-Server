/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledExecutorService;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.MonitoredS3ObjectConfiguration;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;

public class TorExitNodeManagerTest extends AbstractRedisClusterTest {

  @Test
  public void testIsTorExitNode() {
    final MonitoredS3ObjectConfiguration configuration = new MonitoredS3ObjectConfiguration();
    configuration.setS3Region("ap-northeast-3");

    final TorExitNodeManager torExitNodeManager =
        new TorExitNodeManager(mock(ScheduledExecutorService.class), configuration);

    assertFalse(torExitNodeManager.isTorExitNode("10.0.0.1"));
    assertFalse(torExitNodeManager.isTorExitNode("10.0.0.2"));

    torExitNodeManager.handleExitListChangedStream(
        new ByteArrayInputStream("10.0.0.1\n10.0.0.2".getBytes(StandardCharsets.UTF_8)));

    assertTrue(torExitNodeManager.isTorExitNode("10.0.0.1"));
    assertTrue(torExitNodeManager.isTorExitNode("10.0.0.2"));
    assertFalse(torExitNodeManager.isTorExitNode("10.0.0.3"));
  }
}
