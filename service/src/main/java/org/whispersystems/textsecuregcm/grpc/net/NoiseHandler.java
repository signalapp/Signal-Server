/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import com.southernstorm.noise.protocol.CipherState;
import com.southernstorm.noise.protocol.CipherStatePair;
import com.southernstorm.noise.protocol.Noise;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.PromiseCombiner;
import javax.crypto.BadPaddingException;
import javax.crypto.ShortBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A bidirectional {@link io.netty.channel.ChannelHandler} that  decrypts inbound messages, and encrypts outbound
 * messages
 */
public class NoiseHandler extends ChannelDuplexHandler {

  private static final Logger log = LoggerFactory.getLogger(NoiseHandler.class);
  private final CipherStatePair cipherStatePair;

  NoiseHandler(CipherStatePair cipherStatePair) {
    this.cipherStatePair = cipherStatePair;
  }

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) throws Exception {
    try {
      if (message instanceof ByteBuf frame) {
        if (frame.readableBytes() > Noise.MAX_PACKET_LEN) {
          throw new NoiseException("Invalid noise message length " + frame.readableBytes());
        }
        // We've read this frame off the wire, and so it's most likely a direct buffer that's not backed by an array.
        // We'll need to copy it to a heap buffer.
        handleInboundDataMessage(context, ByteBufUtil.getBytes(frame));
      } else {
        // Anything except ByteBufs should have been filtered out of the pipeline by now; treat this as an error
        throw new IllegalArgumentException("Unexpected message in pipeline: " + message);
      }
    } finally {
      ReferenceCountUtil.release(message);
    }
  }


  private void handleInboundDataMessage(final ChannelHandlerContext context, final byte[] frameBytes)
      throws ShortBufferException, BadPaddingException {
    final CipherState cipherState = cipherStatePair.getReceiver();
    // Overwrite the ciphertext with the plaintext to avoid an extra allocation for a dedicated plaintext buffer
    final int plaintextLength = cipherState.decryptWithAd(null,
        frameBytes, 0,
        frameBytes, 0,
        frameBytes.length);

    // Forward the decrypted plaintext along
    context.fireChannelRead(Unpooled.wrappedBuffer(frameBytes, 0, plaintextLength));
  }

  @Override
  public void write(final ChannelHandlerContext context, final Object message, final ChannelPromise promise)
      throws Exception {
    if (message instanceof ByteBuf byteBuf) {
      try {
        // TODO Buffer/consolidate Noise writes to avoid sending a bazillion tiny (or empty) frames
        final CipherState cipherState = cipherStatePair.getSender();

        // Server message might not fit in a single noise packet, break it up into as many chunks as we need
        final PromiseCombiner pc = new PromiseCombiner(context.executor());
        while (byteBuf.isReadable()) {
          final ByteBuf plaintext = byteBuf.readSlice(Math.min(
              // need room for a 16-byte AEAD tag
              Noise.MAX_PACKET_LEN - 16,
              byteBuf.readableBytes()));

          final int plaintextLength = plaintext.readableBytes();

          // We've read these bytes from a local connection; although that likely means they're backed by a heap array, the
          // buffer is read-only and won't grant us access to the underlying array. Instead, we need to copy the bytes to a
          // mutable array. We also want to encrypt in place, so we allocate enough extra space for the trailing MAC.
          final byte[] noiseBuffer = new byte[plaintext.readableBytes() + cipherState.getMACLength()];
          plaintext.readBytes(noiseBuffer, 0, plaintext.readableBytes());

          // Overwrite the plaintext with the ciphertext to avoid an extra allocation for a dedicated ciphertext buffer
          cipherState.encryptWithAd(null, noiseBuffer, 0, noiseBuffer, 0, plaintextLength);

          pc.add(context.write(Unpooled.wrappedBuffer(noiseBuffer)));
        }
        pc.finish(promise);
      } finally {
        ReferenceCountUtil.release(byteBuf);
      }
    } else {
      if (!(message instanceof OutboundCloseErrorMessage)) {
        // Downstream handlers may write OutboundCloseErrorMessages that don't need to be encrypted (e.g. "close" frames
        // that get issued in response to exceptions)
        log.warn("Unexpected object in pipeline: {}", message);
      }
      context.write(message, promise);
    }
  }

  @Override
  public void handlerRemoved(ChannelHandlerContext var1) {
    if (cipherStatePair != null) {
      cipherStatePair.destroy();
    }
  }

}
