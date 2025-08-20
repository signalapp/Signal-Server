/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.common.annotations.VisibleForTesting;
import io.grpc.*;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class MetricServerInterceptor implements ServerInterceptor {

  private static final String TAG_SERVICE_NAME = "grpcService";
  private static final String TAG_METHOD_NAME = "method";
  private static final String TAG_METHOD_TYPE = "methodType";
  private static final String TAG_STATUS_CODE = "statusCode";

  @VisibleForTesting
  static final String REQUEST_MESSAGE_COUNTER_NAME = MetricsUtil.name(MetricServerInterceptor.class, "requestMessage");
  @VisibleForTesting
  static final String RESPONSE_COUNTER_NAME = MetricsUtil.name(MetricServerInterceptor.class, "responseMessage");
  @VisibleForTesting
  static final String RPC_COUNTER_NAME = MetricsUtil.name(MetricServerInterceptor.class, "rpc");
  @VisibleForTesting
  static final String DURATION_TIMER_NAME = MetricsUtil.name(MetricServerInterceptor.class, "processingDuration");

  private final MeterRegistry meterRegistry;
  private final ClientReleaseManager clientReleaseManager;

  public MetricServerInterceptor(final MeterRegistry meterRegistry, final ClientReleaseManager clientReleaseManager) {
    this.meterRegistry = meterRegistry;
    this.clientReleaseManager = clientReleaseManager;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(
      final ServerCall<ReqT, RespT> call,
      final Metadata headers,
      final ServerCallHandler<ReqT, RespT> next) {

    final Optional<String> userAgentString = RequestAttributesUtil.getUserAgent();
    final List<Tag> tagList = new ArrayList<>(6);
    tagList.add(Tag.of(TAG_SERVICE_NAME, call.getMethodDescriptor().getServiceName()));
    tagList.add(Tag.of(TAG_METHOD_NAME, call.getMethodDescriptor().getBareMethodName()));
    tagList.add(Tag.of(TAG_METHOD_TYPE, call.getMethodDescriptor().getType().name()));

    RequestAttributesUtil.getUserAgent()
        .map(UserAgentTagUtil::getLibsignalAndPlatformTags)
        .ifPresent(tagList::addAll);
    userAgentString
        .flatMap(ua -> UserAgentTagUtil.getClientVersionTag(ua, clientReleaseManager))
        .ifPresent(tagList::add);

    final Tags tags = Tags.of(tagList);


    final MetricServerCall<ReqT, RespT> monitoringServerCall = new MetricServerCall<>(call, tags);
    return new MetricServerCallListener<>(next.startCall(monitoringServerCall, headers), tags);
  }

  /**
   * A ServerCall delegator that updates metrics on response messages and the final RPC status
   */
  private class MetricServerCall<ReqT, RespT> extends ForwardingServerCall.SimpleForwardingServerCall<ReqT, RespT> {

    private final Counter responseMessageCounter;
    private final Tags tags;

    MetricServerCall(final ServerCall<ReqT, RespT> delegate, final Tags tags) {
      super(delegate);
      this.responseMessageCounter = meterRegistry.counter(RESPONSE_COUNTER_NAME, tags);
      this.tags = tags;
    }

    @Override
    public void close(final Status status, final Metadata responseHeaders) {
      meterRegistry.counter(RPC_COUNTER_NAME, tags.and(TAG_STATUS_CODE, status.getCode().name())).increment();
      super.close(status, responseHeaders);
    }

    @Override
    public void sendMessage(final RespT responseMessage) {
      this.responseMessageCounter.increment();
      super.sendMessage(responseMessage);
    }
  }

  /**
   * A ServerCallListener delegator that updates metrics on requests and measures the RPC time on completion
   */
  private class MetricServerCallListener<ReqT> extends ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT> {

    private final Counter requestCounter;
    private final Timer responseTimer;
    private final Timer.Sample sample;

    MetricServerCallListener(final ServerCall.Listener<ReqT> delegate, final Tags tags) {
      super(delegate);
      this.requestCounter = meterRegistry.counter(REQUEST_MESSAGE_COUNTER_NAME, tags);
      this.responseTimer = meterRegistry.timer(DURATION_TIMER_NAME, tags);
      this.sample = Timer.start(meterRegistry);
    }

    @Override
    public void onMessage(final ReqT requestMessage) {
      this.requestCounter.increment();
      super.onMessage(requestMessage);
    }

    @Override
    public void onComplete() {
      this.sample.stop(responseTimer);
      super.onComplete();
    }

    @Override
    public void onCancel() {
      this.sample.stop(responseTimer);
      super.onCancel();
    }
  }
}
