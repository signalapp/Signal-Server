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
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http.websocketx.BinaryWebSocketFrame;
import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.PromiseCombiner;
import io.netty.util.internal.EmptyArrays;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.crypto.BadPaddingException;
import javax.crypto.ShortBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.util.ExceptionUtils;

/**
 * A bidirectional {@link io.netty.channel.ChannelHandler} that establishes a noise session with an initiator, decrypts
 * inbound messages, and encrypts outbound messages
 */
abstract class NoiseHandler extends ChannelDuplexHandler {

  private static final Logger log = LoggerFactory.getLogger(NoiseHandler.class);

  private enum State {
    // Waiting for handshake to complete
    HANDSHAKE,
    // Can freely exchange encrypted noise messages on an established session
    TRANSPORT,
    // Finished with error
    ERROR
  }

  private final NoiseHandshakeHelper handshakeHelper;

  private State state = State.HANDSHAKE;
  private CipherStatePair cipherStatePair;

  NoiseHandler(NoiseHandshakeHelper handshakeHelper) {
    this.handshakeHelper = handshakeHelper;
  }

  /**
   * The result of processing an initiator handshake payload
   *
   * @param fastOpenRequest     A fast-open request included in the handshake. If none was present, this should be an
   *                            empty ByteBuf
   * @param authenticatedDevice If present, the successfully authenticated initiator identity
   */
  record HandshakeResult(ByteBuf fastOpenRequest, Optional<AuthenticatedDevice> authenticatedDevice) {}

  /**
   * Parse and potentially authenticate the initiator handshake message
   *
   * @param context            A {@link ChannelHandlerContext}
   * @param initiatorPublicKey The initiator's static public key, if a handshake pattern that includes it was used
   * @param handshakePayload   The handshake payload provided in the initiator message
   * @return A {@link HandshakeResult} that includes an authenticated device and a parsed fast-open request if one was
   * present in the handshake payload.
   * @throws NoiseHandshakeException       If the handshake payload was invalid
   * @throws ClientAuthenticationException If the initiatorPublicKey could not be authenticated
   */
  abstract CompletableFuture<HandshakeResult> handleHandshakePayload(
      final ChannelHandlerContext context,
      final Optional<byte[]> initiatorPublicKey,
      final ByteBuf handshakePayload) throws NoiseHandshakeException, ClientAuthenticationException;

  @Override
  public void channelRead(final ChannelHandlerContext context, final Object message) throws Exception {
    try {
      if (message instanceof BinaryWebSocketFrame frame) {
        if (frame.content().readableBytes() > Noise.MAX_PACKET_LEN) {
          final String error = "Invalid noise message length " + frame.content().readableBytes();
          throw state == State.HANDSHAKE ? new NoiseHandshakeException(error) : new NoiseException(error);
        }
        // We've read this frame off the wire, and so it's most likely a direct buffer that's not backed by an array.
        // We'll need to copy it to a heap buffer.
        handleInboundMessage(context, ByteBufUtil.getBytes(frame.content()));
      } else {
        // Anything except binary WebSocket frames should have been filtered out of the pipeline by now; treat this as an
        // error
        throw new IllegalArgumentException("Unexpected message in pipeline: " + message);
      }
    } catch (Exception e) {
      fail(context, e);
    } finally {
      ReferenceCountUtil.release(message);
    }
  }

  private void handleInboundMessage(final ChannelHandlerContext context, final byte[] frameBytes)
      throws NoiseHandshakeException, ShortBufferException, BadPaddingException, ClientAuthenticationException {
    switch (state) {

      // Got an initiator handshake message
      case HANDSHAKE -> {
        final ByteBuf payload = handshakeHelper.read(frameBytes);
        handleHandshakePayload(context, handshakeHelper.remotePublicKey(), payload).whenCompleteAsync(
            (result, throwable) -> {
              if (state == State.ERROR) {
                return;
              }
              if (throwable != null) {
                fail(context, ExceptionUtils.unwrap(throwable));
                return;
              }
              context.fireUserEventTriggered(new NoiseIdentityDeterminedEvent(result.authenticatedDevice()));

              // Now that we've authenticated, write the handshake response
              byte[] handshakeMessage = handshakeHelper.write(EmptyArrays.EMPTY_BYTES);
              context.writeAndFlush(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(handshakeMessage)))
                  .addListener(ChannelFutureListener.FIRE_EXCEPTION_ON_FAILURE);

              // The handshake is complete. We can start intercepting read/write for noise encryption/decryption
              this.state = State.TRANSPORT;
              this.cipherStatePair = handshakeHelper.getHandshakeState().split();
              if (result.fastOpenRequest().isReadable()) {
                // The handshake had a fast-open request. Forward the plaintext of the request to the server, we'll
                // encrypt the response when the server writes back through us
                context.fireChannelRead(result.fastOpenRequest());
              } else {
                ReferenceCountUtil.release(result.fastOpenRequest());
              }
            }, context.executor());
      }

      // Got a client message that should be decrypted and forwarded
      case TRANSPORT -> {
        final CipherState cipherState = cipherStatePair.getReceiver();
        // Overwrite the ciphertext with the plaintext to avoid an extra allocation for a dedicated plaintext buffer
        final int plaintextLength = cipherState.decryptWithAd(null,
            frameBytes, 0,
            frameBytes, 0,
            frameBytes.length);

        // Forward the decrypted plaintext along
        context.fireChannelRead(Unpooled.wrappedBuffer(frameBytes, 0, plaintextLength));
      }

      // The session is already in an error state, drop the message
      case ERROR -> {
      }
    }
  }

  /**
   * Set the state to the error state (so subsequent messages fast-fail) and propagate the failure reason on the
   * context
   */
  private void fail(final ChannelHandlerContext context, final Throwable cause) {
    this.state = State.ERROR;
    context.fireExceptionCaught(cause);
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

          pc.add(context.write(new BinaryWebSocketFrame(Unpooled.wrappedBuffer(noiseBuffer))));
        }
        pc.finish(promise);
      } finally {
        ReferenceCountUtil.release(byteBuf);
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
}
