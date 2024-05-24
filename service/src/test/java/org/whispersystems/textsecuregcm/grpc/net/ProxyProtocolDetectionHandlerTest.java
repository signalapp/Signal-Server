package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.haproxy.HAProxyMessage;
import java.util.HexFormat;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ProxyProtocolDetectionHandlerTest {

  private EmbeddedChannel embeddedChannel;

  private static final byte[] PROXY_V2_MESSAGE_BYTES =
      HexFormat.of().parseHex("0d0a0d0a000d0a515549540a2111000c0a0000010a000002303901bb");

  @BeforeEach
  void setUp() {
    embeddedChannel = new EmbeddedChannel(new ProxyProtocolDetectionHandler());
  }

  @Test
  void singlePacketProxyMessage() throws InterruptedException {
    final ChannelFuture writeFuture = embeddedChannel.writeOneInbound(Unpooled.wrappedBuffer(PROXY_V2_MESSAGE_BYTES));
    embeddedChannel.flushInbound();

    writeFuture.await();

    assertTrue(embeddedChannel.pipeline().toMap().isEmpty());
    assertEquals(1, embeddedChannel.inboundMessages().size());
    assertInstanceOf(HAProxyMessage.class, embeddedChannel.inboundMessages().poll());
  }

  @Test
  void multiPacketProxyMessage() throws InterruptedException {
    final ChannelFuture firstWriteFuture = embeddedChannel.writeOneInbound(
        Unpooled.wrappedBuffer(PROXY_V2_MESSAGE_BYTES, 0,
            ProxyProtocolDetectionHandler.PROXY_MESSAGE_DETECTION_BYTES - 1));

    final ChannelFuture secondWriteFuture = embeddedChannel.writeOneInbound(
        Unpooled.wrappedBuffer(PROXY_V2_MESSAGE_BYTES, ProxyProtocolDetectionHandler.PROXY_MESSAGE_DETECTION_BYTES - 1,
            PROXY_V2_MESSAGE_BYTES.length - (ProxyProtocolDetectionHandler.PROXY_MESSAGE_DETECTION_BYTES - 1)));

    embeddedChannel.flushInbound();

    firstWriteFuture.await();
    secondWriteFuture.await();

    assertTrue(embeddedChannel.pipeline().toMap().isEmpty());
    assertEquals(1, embeddedChannel.inboundMessages().size());
    assertInstanceOf(HAProxyMessage.class, embeddedChannel.inboundMessages().poll());
  }

  @Test
  void singlePacketNonProxyMessage() throws InterruptedException {
    final byte[] nonProxyProtocolMessage = new byte[32];
    ThreadLocalRandom.current().nextBytes(nonProxyProtocolMessage);

    final ChannelFuture writeFuture = embeddedChannel.writeOneInbound(Unpooled.wrappedBuffer(nonProxyProtocolMessage));
    embeddedChannel.flushInbound();

    writeFuture.await();

    assertTrue(embeddedChannel.pipeline().toMap().isEmpty());
    assertEquals(1, embeddedChannel.inboundMessages().size());

    final Object inboundMessage = embeddedChannel.inboundMessages().poll();

    assertInstanceOf(ByteBuf.class, inboundMessage);
    assertArrayEquals(nonProxyProtocolMessage, ByteBufUtil.getBytes((ByteBuf) inboundMessage));
  }

  @Test
  void multiPacketNonProxyMessage() throws InterruptedException {
    final byte[] nonProxyProtocolMessage = new byte[32];
    ThreadLocalRandom.current().nextBytes(nonProxyProtocolMessage);

    final ChannelFuture firstWriteFuture = embeddedChannel.writeOneInbound(
        Unpooled.wrappedBuffer(nonProxyProtocolMessage, 0,
            ProxyProtocolDetectionHandler.PROXY_MESSAGE_DETECTION_BYTES - 1));

    final ChannelFuture secondWriteFuture = embeddedChannel.writeOneInbound(
        Unpooled.wrappedBuffer(nonProxyProtocolMessage, ProxyProtocolDetectionHandler.PROXY_MESSAGE_DETECTION_BYTES - 1,
            nonProxyProtocolMessage.length - (ProxyProtocolDetectionHandler.PROXY_MESSAGE_DETECTION_BYTES - 1)));

    embeddedChannel.flushInbound();

    firstWriteFuture.await();
    secondWriteFuture.await();

    assertTrue(embeddedChannel.pipeline().toMap().isEmpty());
    assertEquals(1, embeddedChannel.inboundMessages().size());

    final Object inboundMessage = embeddedChannel.inboundMessages().poll();

    assertInstanceOf(ByteBuf.class, inboundMessage);
    assertArrayEquals(nonProxyProtocolMessage, ByteBufUtil.getBytes((ByteBuf) inboundMessage));
  }
}
