package org.whispersystems.textsecuregcm.grpc.net.client;

import com.southernstorm.noise.protocol.CipherState;
import com.southernstorm.noise.protocol.CipherStatePair;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.CloseWebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.grpc.net.noisedirect.NoiseDirectFrame;

/**
 * A Noise transport handler manages a bidirectional Noise session after a handshake has completed.
 */
public class NoiseClientTransportHandler extends ChannelDuplexHandler {

  private final CipherStatePair cipherStatePair;

  private static final Logger log = LoggerFactory.getLogger(NoiseClientTransportHandler.class);

  NoiseClientTransportHandler(CipherStatePair cipherStatePair) {
    this.cipherStatePair = cipherStatePair;
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) throws Exception {
    try {
      if (message instanceof ByteBuf frame) {
        final CipherState cipherState = cipherStatePair.getReceiver();

        // We've read this frame off the wire, and so it's most likely a direct buffer that's not backed by an array.
        // We'll need to copy it to a heap buffer.
        final byte[] noiseBuffer = ByteBufUtil.getBytes(frame);

        // Overwrite the ciphertext with the plaintext to avoid an extra allocation for a dedicated plaintext buffer
        final int plaintextLength = cipherState.decryptWithAd(null, noiseBuffer, 0, noiseBuffer, 0, noiseBuffer.length);

        context.fireChannelRead(Unpooled.wrappedBuffer(noiseBuffer, 0, plaintextLength));
      } else {
        // Anything except binary frames should have been filtered out of the pipeline by now; treat this as an
        // error
        throw new IllegalArgumentException("Unexpected message in pipeline: " + message);
      }
    } finally {
      ReferenceCountUtil.release(message);
    }
  }


  @Override
  public void write(final ChannelHandlerContext context, final Object message, final ChannelPromise promise)
      throws Exception {
    if (message instanceof ByteBuf plaintext) {
      try {
        final CipherState cipherState = cipherStatePair.getSender();
        final int plaintextLength = plaintext.readableBytes();

        // We've read these bytes from a local connection; although that likely means they're backed by a heap array, the
        // buffer is read-only and won't grant us access to the underlying array. Instead, we need to copy the bytes to a
        // mutable array. We also want to encrypt in place, so we allocate enough extra space for the trailing MAC.
        final byte[] noiseBuffer = new byte[plaintext.readableBytes() + cipherState.getMACLength()];
        plaintext.readBytes(noiseBuffer, 0, plaintext.readableBytes());

        // Overwrite the plaintext with the ciphertext to avoid an extra allocation for a dedicated ciphertext buffer
        cipherState.encryptWithAd(null, noiseBuffer, 0, noiseBuffer, 0, plaintextLength);

        context.write(Unpooled.wrappedBuffer(noiseBuffer), promise);
      } finally {
        ReferenceCountUtil.release(plaintext);
      }
    } else {
      if (!(message instanceof CloseWebSocketFrame || message instanceof NoiseDirectFrame)) {
        // Clients only write ByteBufs or a close frame on errors, so any other message is unexpected
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
