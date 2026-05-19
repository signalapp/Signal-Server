/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.channel.WriteBufferWaterMark;
import io.netty.channel.embedded.EmbeddedChannel;
import java.nio.charset.StandardCharsets;
import io.netty.util.test.LeakPresenceExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LeakPresenceExtension.class)
class H2FrameProxyHandlerTest {
  @Test
  void proxyWritabilityChanged() {
    final EmbeddedChannel target = new EmbeddedChannel();
    final EmbeddedChannel source = new EmbeddedChannel(new H2FrameProxyHandler(target, "test"));

    // Set a tiny watermark to guarantee an unflushed write sets to unwritable, and then buffer some data
    final byte[] bufferedData = "8 bytes!".getBytes(StandardCharsets.UTF_8);
    target.config().setWriteBufferWaterMark(new WriteBufferWaterMark(4, 8));
    target.write(bufferedData);

    assertNull(target.readOutbound(), "nothing should be written without a flush");
    assertFalse(target.isWritable(), "target should be unwritable because we've buffered more than the high watermark");
    assertFalse(source.config().isAutoRead(), "source should not read because the target is unwritable");

    target.flush();
    assertTrue(target.isWritable(), "after a flush, the target should be writable");
    assertTrue(source.config().isAutoRead(), "after the target becomes writable, autoRead should be enabled");
  }
}
