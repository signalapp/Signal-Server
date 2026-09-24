/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc.net;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

/// Counts completed TLS handshakes, tagged with `success` and `exception`.
///
/// Must be added after the [io.netty.handler.ssl.SniHandler] or [SslHandler]; removes itself once the handshake completes.
@ChannelHandler.Sharable
public class TlsHandshakeMetricsHandler extends ChannelInboundHandlerAdapter {

  @VisibleForTesting
  static final String HANDSHAKE_COUNTER_NAME = MetricsUtil.name(TlsHandshakeMetricsHandler.class, "handshake");

  @VisibleForTesting
  static final String SUCCESS_TAG_NAME = "success";

  @VisibleForTesting
  static final String EXCEPTION_TAG_NAME = "exception";

  private final MeterRegistry meterRegistry;

  public TlsHandshakeMetricsHandler(final MeterRegistry meterRegistry) {
    this.meterRegistry = meterRegistry;
  }

  @Override
  public void userEventTriggered(final ChannelHandlerContext ctx, final Object evt) throws Exception {
    if (evt instanceof SslHandshakeCompletionEvent handshakeCompletionEvent) {
      final Tags tags;

      if (handshakeCompletionEvent.isSuccess()) {
        tags = Tags.of(SUCCESS_TAG_NAME, "true",
            EXCEPTION_TAG_NAME, "none");
      } else {
        tags = Tags.of(SUCCESS_TAG_NAME, "false",
            EXCEPTION_TAG_NAME, handshakeCompletionEvent.cause().getClass().getSimpleName());
      }

      meterRegistry.counter(HANDSHAKE_COUNTER_NAME, tags).increment();
      ctx.pipeline().remove(this);
    }

    super.userEventTriggered(ctx, evt);
  }

}
