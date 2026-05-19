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
import io.netty.util.ReferenceCountUtil;
import io.netty.util.test.LeakPresenceExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LeakPresenceExtension.class)
class ProxyProtocolHandlerTest {

  private static final HAProxyProtocolVersion PROTOCOL_VERSION = HAProxyProtocolVersion.V2;
  private static final String SOURCE_ADDRESS = "10.0.0.1";
  private static final String DESTINATION_ADDRESS = "10.0.0.2";

  private static HAProxyMessage proxyMessage() {
    return new HAProxyMessage(
        PROTOCOL_VERSION, HAProxyCommand.PROXY,
        HAProxyProxiedProtocol.TCP4, SOURCE_ADDRESS, DESTINATION_ADDRESS, 1234, 5678);
  }

  @Test
  void sendHeader() {
    final HAProxyMessage proxyMessage = proxyMessage();
    final EmbeddedChannel encoder = new EmbeddedChannel(HAProxyMessageEncoder.INSTANCE);
    encoder.writeOutbound(proxyMessage);
    final ByteBuf ppv2Bytes = encoder.readOutbound();

    final EmbeddedChannel ch = new EmbeddedChannel(new ProxyProtocolHandler());
    ch.writeInbound(ppv2Bytes);
    final HAProxyMessage actual = ch.readInbound();
    try {
      assertEquals(PROTOCOL_VERSION, actual.protocolVersion());
      assertEquals(SOURCE_ADDRESS, actual.sourceAddress());
    } finally {
      ReferenceCountUtil.release(actual);
    }
  }

  @Test
  void sendHeaderSlowly() {
    final HAProxyMessage proxyMessage = proxyMessage();
    final EmbeddedChannel encoder = new EmbeddedChannel(HAProxyMessageEncoder.INSTANCE);
    encoder.writeOutbound(proxyMessage);

    final EmbeddedChannel ch = new EmbeddedChannel(new ProxyProtocolHandler());
    final ByteBuf ppv2Bytes = encoder.readOutbound();
    try {
      while (ppv2Bytes.isReadable()) {
        assertNull(ch.readInbound());
        ch.writeInbound(ppv2Bytes.readBytes(1));
      }
    } finally {
      ReferenceCountUtil.release(ppv2Bytes);
    }

    final HAProxyMessage actual = ch.readInbound();
    try {
      assertEquals(PROTOCOL_VERSION, actual.protocolVersion());
      assertEquals(SOURCE_ADDRESS, actual.sourceAddress());
    } finally {
      ReferenceCountUtil.release(actual);
    }
  }
}
