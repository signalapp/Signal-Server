/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyMessageEncoder;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import org.junit.jupiter.api.Test;

class ProxyProtocolHandlerTest extends AbstractLeakDetectionTest {
  private static final HAProxyMessage PROXY_MESSAGE = new HAProxyMessage(
      HAProxyProtocolVersion.V2, HAProxyCommand.PROXY,
      HAProxyProxiedProtocol.TCP4, "10.0.0.1", "10.0.0.2", 1234, 5678);

  @Test
  void sendHeader() {
    final EmbeddedChannel encoder = new EmbeddedChannel(HAProxyMessageEncoder.INSTANCE);
    encoder.writeOutbound(PROXY_MESSAGE.retain());
    final ByteBuf ppv2Bytes = encoder.readOutbound();

    final EmbeddedChannel ch = new EmbeddedChannel(new ProxyProtocolHandler());
    ch.writeInbound(ppv2Bytes);
    final HAProxyMessage actual = ch.readInbound();
    assertEquals(PROXY_MESSAGE.protocolVersion(), actual.protocolVersion());
    assertEquals(PROXY_MESSAGE.sourceAddress(), actual.sourceAddress());
  }

  @Test
  void sendHeaderSlowly() {
    final EmbeddedChannel encoder = new EmbeddedChannel(HAProxyMessageEncoder.INSTANCE);
    encoder.writeOutbound(PROXY_MESSAGE.retain());
    final ByteBuf ppv2Bytes = encoder.readOutbound();

    final EmbeddedChannel ch = new EmbeddedChannel(new ProxyProtocolHandler());
    while (ppv2Bytes.isReadable()) {
      assertNull(ch.readInbound());
      ch.writeInbound(ppv2Bytes.readBytes(1));
    }

    final HAProxyMessage actual = ch.readInbound();
    assertEquals(PROXY_MESSAGE.protocolVersion(), actual.protocolVersion());
    assertEquals(PROXY_MESSAGE.sourceAddress(), actual.sourceAddress());
  }
}
