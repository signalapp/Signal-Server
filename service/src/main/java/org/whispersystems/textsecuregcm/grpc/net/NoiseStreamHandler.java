package org.whispersystems.textsecuregcm.grpc.net;

import com.southernstorm.noise.protocol.CipherState;
import com.southernstorm.noise.protocol.CipherStatePair;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import javax.crypto.BadPaddingException;
import javax.crypto.ShortBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Noise stream handler manages a bidirectional Noise session after a handshake has completed.
 */
class NoiseStreamHandler extends ChannelDuplexHandler {

  private final CipherStatePair cipherStatePair;

  private static final Logger log = LoggerFactory.getLogger(NoiseStreamHandler.class);

  NoiseStreamHandler(CipherStatePair cipherStatePair) {
    this.cipherStatePair = cipherStatePair;
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message)
      throws ShortBufferException, BadPaddingException {

    if (message instanceof BinaryWebSocketFrame frame) {
      try {
        final CipherState cipherState = cipherStatePair.getReceiver();

        // We've read this frame off the wire, and so it's most likely a direct buffer that's not backed by an array.
        // We'll need to copy it to a heap buffer.
        final byte[] noiseBuffer = ByteBufUtil.getBytes(frame.content());

        // Overwrite the ciphertext with the plaintext to avoid an extra allocation for a dedicated plaintext buffer
        final int plaintextLength = cipherState.decryptWithAd(null, noiseBuffer, 0, noiseBuffer, 0, noiseBuffer.length);

        context.fireChannelRead(Unpooled.wrappedBuffer(noiseBuffer, 0, plaintextLength));
      } finally {
        frame.release();
      }
    } else {
      // Anything except binary WebSocket frames should have been filtered out of the pipeline by now; treat this as an
      // error
      ReferenceCountUtil.release(message);
      throw new IllegalArgumentException("Unexpected message in pipeline: " + message);
    }
  }

  @Override
  public void write(final ChannelHandlerContext context, final Object message, final ChannelPromise promise) throws Exception {
    if (message instanceof ByteBuf plaintext) {
      try {
        // TODO Buffer/consolidate Noise writes to avoid sending a bazillion tiny (or empty) frames
        final CipherState cipherState = cipherStatePair.getSender();
        final int plaintextLength = plaintext.readableBytes();

        // We've read these bytes from a local connection; although that likely means they're backed by a heap array, the
        // buffer is read-only and won't grant us access to the underlying array. Instead, we need to copy the bytes to a
        // mutable array. We also want to encrypt in place, so we allocate enough extra space for the trailing MAC.
        final byte[] noiseBuffer = new byte[plaintext.readableBytes() + cipherState.getMACLength()];
        plaintext.readBytes(noiseBuffer, 0, plaintext.readableBytes());

        // Overwrite the plaintext with the ciphertext to avoid an extra allocation for a dedicated ciphertext buffer
        cipherState.encryptWithAd(null, noiseBuffer, 0, noiseBuffer, 0, plaintextLength);

        context.write(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(noiseBuffer)), promise);
      } finally {
        plaintext.release();
      }
    } else {
      if (!(message instanceof WebSocketFrame)) {
        // Downstream handlers may write WebSocket frames that don't need to be encrypted (e.g. "close" frames that
        // get issued in response to exceptions)
        log.warn("Unexpected object in pipeline: {}", message);
      }

      context.write(message, promise);
    }
  }

  @Override
  public void handlerRemoved(final ChannelHandlerContext context) {
    cipherStatePair.destroy();
  }
}
