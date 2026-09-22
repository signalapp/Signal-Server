/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Metrics;
import java.util.concurrent.atomic.AtomicInteger;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;

/// Limits the number of concurrent gRPC calls. Returns `io.grpc.Status.UNAVAILABLE` if the limit is exceeded. This
/// interceptor is meant to be used alongside a virtual thread executor. Unfortunately, we can't re-use
/// [org.whispersystems.textsecuregcm.util.BoundedVirtualThreadFactory] directly because the gRPC stack does not
/// gracefully handle [java.util.concurrent.RejectedExecutionException]s (see
/// [issue](https://github.com/grpc/grpc-java/issues/636))
public class ConcurrentCallLimitingInterceptor implements ServerInterceptor {

  private final int maxConcurrentCalls;

  private final AtomicInteger concurrentCalls = new AtomicInteger(0);
  private final Counter rejectedCallCounter;

  public ConcurrentCallLimitingInterceptor(final int maxConcurrentCalls) {
    this.maxConcurrentCalls = maxConcurrentCalls;

    Metrics.gauge(MetricsUtil.name(ConcurrentCallLimitingInterceptor.class, "active"),
        concurrentCalls, AtomicInteger::doubleValue);
    this.rejectedCallCounter =
        Metrics.counter(MetricsUtil.name(ConcurrentCallLimitingInterceptor.class, "rejected"));
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers, final ServerCallHandler<ReqT, RespT> next) {

    if (concurrentCalls.incrementAndGet() > maxConcurrentCalls) {
      concurrentCalls.decrementAndGet();
      rejectedCallCounter.increment();
      return ServerInterceptorUtil.closeWithStatusException(call, GrpcExceptions.unavailable());
    }

    try {
      return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {
        @Override
        public void onComplete() {
          concurrentCalls.decrementAndGet();
          super.onComplete();
        }

        @Override
        public void onCancel() {
          concurrentCalls.decrementAndGet();
          super.onCancel();
        }
      };
    } catch (final Exception e) {
      concurrentCalls.decrementAndGet();
      throw e;
    }
  }

  @VisibleForTesting
  int getActiveCalls() {
    return concurrentCalls.get();
  }
}
