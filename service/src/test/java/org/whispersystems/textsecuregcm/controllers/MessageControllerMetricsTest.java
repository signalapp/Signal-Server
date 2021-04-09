/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.push.ApnFallbackManager;
import org.whispersystems.textsecuregcm.push.MessageSender;
import org.whispersystems.textsecuregcm.push.ReceiptSender;
import org.whispersystems.textsecuregcm.redis.AbstractRedisClusterTest;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.storage.MessagesManager;

import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class MessageControllerMetricsTest extends AbstractRedisClusterTest {

  private MessageController messageController;

  @Before
  public void setUp() throws Exception {
    super.setUp();

    messageController = new MessageController(mock(RateLimiters.class),
        mock(MessageSender.class),
        mock(ReceiptSender.class),
        mock(AccountsManager.class),
        mock(MessagesManager.class),
        mock(ApnFallbackManager.class),
        mock(DynamicConfigurationManager.class),
        getRedisCluster(),
        mock(ScheduledExecutorService.class));
  }

  @Test
  public void testRecordInternationalUnsealedSenderMetrics() {
    final String senderIp = "127.0.0.1";

    messageController.recordInternationalUnsealedSenderMetrics(senderIp, "84", "+18005551234");
    messageController.recordInternationalUnsealedSenderMetrics(senderIp, "84", "+18005551234");

    getRedisCluster().useCluster(connection -> {
      assertEquals(1, (long)connection.sync().pfcount(MessageController.getDestinationSetKey(senderIp)));
      assertEquals(2, Long.parseLong(connection.sync().get(MessageController.getMessageCountKey(senderIp)), 10));

      assertTrue(connection.sync().ttl(MessageController.getDestinationSetKey(senderIp)) >= 0);
      assertTrue(connection.sync().ttl(MessageController.getMessageCountKey(senderIp)) >= 0);
    });
  }
}
