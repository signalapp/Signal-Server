package org.whispersystems.textsecuregcm.grpc.net;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.southernstorm.noise.protocol.HandshakeState;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import java.security.NoSuchAlgorithmException;
import java.util.Optional;
import javax.crypto.BadPaddingException;
import javax.crypto.ShortBufferException;
import org.junit.jupiter.api.Test;
import org.signal.libsignal.protocol.ecc.ECKeyPair;

class NoiseNXHandshakeHandlerTest extends AbstractNoiseHandshakeHandlerTest {

  @Override
  protected NoiseNXHandshakeHandler getHandler(final ECKeyPair serverKeyPair,
      final byte[] serverPublicKeySignature) {

    return new NoiseNXHandshakeHandler(serverKeyPair, serverPublicKeySignature);
  }

  @Test
  void handleCompleteHandshake()
      throws NoSuchAlgorithmException, ShortBufferException, InterruptedException, BadPaddingException {

    final EmbeddedChannel embeddedChannel = getEmbeddedChannel();

    assertNotNull(embeddedChannel.pipeline().get(NoiseNXHandshakeHandler.class));

    final HandshakeState clientHandshakeState =
        new HandshakeState(NoiseNXHandshakeHandler.NOISE_PROTOCOL_NAME, HandshakeState.INITIATOR);

    clientHandshakeState.start();

    {
      final byte[] ephemeralKeyMessageBytes = new byte[32];
      clientHandshakeState.writeMessage(ephemeralKeyMessageBytes, 0, null, 0, 0);

      final BinaryWebSocketFrame ephemeralKeyMessageFrame =
          new BinaryWebSocketFrame(Unpooled.wrappedBuffer(ephemeralKeyMessageBytes));

      assertTrue(embeddedChannel.writeOneInbound(ephemeralKeyMessageFrame).await().isSuccess());
      assertEquals(0, ephemeralKeyMessageFrame.refCnt());
    }

    {
      assertEquals(1, embeddedChannel.outboundMessages().size());

      final BinaryWebSocketFrame serverStaticKeyMessageFrame =
          (BinaryWebSocketFrame) embeddedChannel.outboundMessages().poll();

      @SuppressWarnings("DataFlowIssue") final byte[] serverStaticKeyMessageBytes =
          new byte[serverStaticKeyMessageFrame.content().readableBytes()];

      serverStaticKeyMessageFrame.content().readBytes(serverStaticKeyMessageBytes);

      final byte[] serverPublicKeySignature = new byte[64];

      final int payloadLength =
          clientHandshakeState.readMessage(serverStaticKeyMessageBytes, 0, serverStaticKeyMessageBytes.length, serverPublicKeySignature, 0);

      assertEquals(serverPublicKeySignature.length, payloadLength);

      final byte[] serverPublicKey = new byte[32];
      clientHandshakeState.getRemotePublicKey().getPublicKey(serverPublicKey, 0);

      assertTrue(getRootPublicKey().verifySignature(serverPublicKey, serverPublicKeySignature));
    }

    assertEquals(new NoiseHandshakeCompleteEvent(Optional.empty()), getNoiseHandshakeCompleteEvent());

    assertNull(embeddedChannel.pipeline().get(NoiseNXHandshakeHandler.class),
        "Handshake handler should remove self from pipeline after successful handshake");

    assertNotNull(embeddedChannel.pipeline().get(NoiseStreamHandler.class),
        "Handshake handler should insert a Noise stream handler after successful handshake");
  }
}
