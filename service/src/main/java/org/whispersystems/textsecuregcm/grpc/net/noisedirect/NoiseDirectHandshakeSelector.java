/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net.noisedirect;

import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import org.signal.libsignal.protocol.ecc.ECKeyPair;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;
import org.whispersystems.textsecuregcm.grpc.net.NoiseAnonymousHandler;
import org.whispersystems.textsecuregcm.grpc.net.NoiseAuthenticatedHandler;
import org.whispersystems.textsecuregcm.grpc.net.NoiseHandshakeException;
import org.whispersystems.textsecuregcm.storage.ClientPublicKeysManager;

import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Waits for a Handshake {@link NoiseDirectFrame} and then installs a {@link NoiseDirectDataFrameCodec} and
 * {@link org.whispersystems.textsecuregcm.grpc.net.NoiseHandler} and removes itself
 */
public class NoiseDirectHandshakeSelector extends ChannelInboundHandlerAdapter {

  private final ClientPublicKeysManager clientPublicKeysManager;
  private final ECKeyPair ecKeyPair;

  public NoiseDirectHandshakeSelector(final ClientPublicKeysManager clientPublicKeysManager, final ECKeyPair ecKeyPair) {
    this.clientPublicKeysManager = clientPublicKeysManager;
    this.ecKeyPair = ecKeyPair;
  }


  @Override
  public void channelRead(final ChannelHandlerContext ctx, final Object msg) throws Exception {
    if (msg instanceof NoiseDirectFrame frame) {
      try {
        // We've received an inbound handshake frame so we know what kind of NoiseHandler we need (authenticated or
        // anonymous). We construct it here, and then remember the handshake type so we can annotate our handshake
        // response with the correct frame type whenever we receive it.
        final ChannelDuplexHandler noiseHandler = switch (frame.frameType()) {
          case DATA, ERROR ->
              throw new NoiseHandshakeException("Invalid frame type for first message " + frame.frameType());
          case IK_HANDSHAKE -> new NoiseAuthenticatedHandler(clientPublicKeysManager, ecKeyPair);
          case NK_HANDSHAKE -> new NoiseAnonymousHandler(ecKeyPair);
        };
        if (ctx.channel().remoteAddress() instanceof InetSocketAddress inetSocketAddress) {
          // TODO: Provide connection metadata / headers in handshake payload
          GrpcClientConnectionManager.handleHandshakeInitiated(ctx.channel(),
              inetSocketAddress.getAddress(),
              "NoiseDirect",
              "");

        } else {
          throw new IOException("Could not determine remote address");
        }

        // Subsequent inbound messages and outbound should be data type frames or close frames. Inbound data frames
        // should be unwrapped and forwarded to the noise handler, outbound buffers should be wrapped and forwarded
        // for network serialization. Note that we need to install the Data frame handler before firing the read,
        // because we may receive an outbound message from the noiseHandler
        ctx.pipeline().addAfter(ctx.name(), null, noiseHandler);
        ctx.pipeline().replace(ctx.name(), null, new NoiseDirectDataFrameCodec());
        ctx.fireChannelRead(frame.content());
      } catch (Exception e) {
        ReferenceCountUtil.release(msg);
        throw e;
      }
    } else {
      ctx.fireChannelRead(msg);
    }
  }
}
