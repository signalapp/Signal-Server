package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.haproxy.HAProxyCommand;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import io.netty.handler.codec.haproxy.HAProxyProtocolVersion;
import io.netty.handler.codec.haproxy.HAProxyProxiedProtocol;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HAProxyMessageHandlerTest {

  private EmbeddedChannel embeddedChannel;

  @BeforeEach
  void setUp() {
    embeddedChannel = new EmbeddedChannel(new HAProxyMessageHandler());
  }

  @Test
  void handleHAProxyMessage() throws InterruptedException {
    final HAProxyMessage haProxyMessage = new HAProxyMessage(
        HAProxyProtocolVersion.V2, HAProxyCommand.PROXY, HAProxyProxiedProtocol.TCP4,
        "10.0.0.1", "10.0.0.2", 12345, 443);

    final ChannelFuture writeFuture = embeddedChannel.writeOneInbound(haProxyMessage);
    embeddedChannel.flushInbound();

    writeFuture.await();

    assertTrue(embeddedChannel.inboundMessages().isEmpty());
    assertEquals(0, haProxyMessage.refCnt());

    assertTrue(embeddedChannel.pipeline().toMap().isEmpty());
  }

  @Test
  void handleNonHAProxyMessage() throws InterruptedException {
    final byte[] bytes = new byte[32];
    ThreadLocalRandom.current().nextBytes(bytes);

    final ByteBuf message = Unpooled.wrappedBuffer(bytes);

    final ChannelFuture writeFuture = embeddedChannel.writeOneInbound(message);
    embeddedChannel.flushInbound();

    writeFuture.await();

    assertEquals(1, embeddedChannel.inboundMessages().size());
    assertEquals(message, embeddedChannel.inboundMessages().poll());
    assertEquals(1, message.refCnt());

    assertTrue(embeddedChannel.pipeline().toMap().isEmpty());
  }
}
