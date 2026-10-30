/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Context;
import io.grpc.Metadata;
import io.grpc.ServerStreamTracer;
import io.micrometer.core.instrument.Counter;
import java.util.Optional;
import javax.annotation.Nullable;

/// Records on-the-wire (i.e. possibly compressed) sizes of response messages.
///
/// Stream tracers are created before the method being called has been resolved. The tracer makes itself available
/// via the call's [Context], and [MetricServerInterceptor] (which only runs for registered methods) supplies the
/// counter to increment; without it, nothing is recorded.
public class MetricServerStreamTracer extends ServerStreamTracer {

  private static final Context.Key<MetricServerStreamTracer> CONTEXT_KEY = Context.key("metricServerStreamTracer");

  @Nullable
  private volatile Counter responseBytesCounter;

  public static class Factory extends ServerStreamTracer.Factory {

    @Override
    public ServerStreamTracer newServerStreamTracer(final String fullMethodName, final Metadata headers) {
      return new MetricServerStreamTracer();
    }
  }

  /// @return the tracer for the current call, if a [Factory] is installed on the server
  static Optional<MetricServerStreamTracer> current() {
    return Optional.ofNullable(CONTEXT_KEY.get());
  }

  void setResponseBytesCounter(final Counter responseBytesCounter) {
    this.responseBytesCounter = responseBytesCounter;
  }

  @Override
  public Context filterContext(final Context context) {
    return context.withValue(CONTEXT_KEY, this);
  }

  @Override
  public void outboundWireSize(final long bytes) {
    final Counter counter = responseBytesCounter;
    if (counter != null) {
      counter.increment(bytes);
    }
  }
}
