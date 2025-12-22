/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.Descriptors;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.rpc.ErrorInfo;
import io.grpc.ForwardingServerCall;
import io.grpc.ForwardingServerCallListener;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.protobuf.StatusProto;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;

public class MetricServerInterceptor implements ServerInterceptor {

  private static final Logger log = LoggerFactory.getLogger(MetricServerInterceptor.class);

  private static final String TAG_SERVICE_NAME = "grpcService";
  private static final String TAG_METHOD_NAME = "method";
  private static final String TAG_METHOD_TYPE = "methodType";
  private static final String TAG_STATUS_CODE = "statusCode";
  private static final String TAG_REASON = "reason";

  @VisibleForTesting
  static final String DEFAULT_SUCCESS_REASON = "success";
  @VisibleForTesting
  static final String DEFAULT_ERROR_REASON = "n/a";

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
    private @Nullable String reason = null;

    MetricServerCall(final ServerCall<ReqT, RespT> delegate, final Tags tags) {
      super(delegate);
      this.responseMessageCounter = meterRegistry.counter(RESPONSE_COUNTER_NAME, tags);
      this.tags = tags;
    }

    @Override
    public void close(final Status status, final Metadata responseHeaders) {
      if (!status.isOk()) {
        reason = errorInfo(StatusProto.fromStatusAndTrailers(status, responseHeaders))
            .map(ErrorInfo::getReason)
            .orElse(DEFAULT_ERROR_REASON);
      }
      Tags responseTags = tags.and(Tag.of(TAG_STATUS_CODE, status.getCode().name()));
      if (reason != null) {
        responseTags = responseTags.and(TAG_REASON, reason);
      }
      meterRegistry.counter(RPC_COUNTER_NAME, responseTags).increment();
      super.close(status, responseHeaders);
    }

    @Override
    public void sendMessage(final RespT responseMessage) {
      this.responseMessageCounter.increment();
      // Extract the annotated reason (if any) from the message
      final String messageReason = MetricServerCall.reason(responseMessage);

      // If there are multiple messages sent on this RPC (server-side streaming), just use the most recent reason
      this.reason = messageReason == null ? DEFAULT_SUCCESS_REASON : messageReason;

      super.sendMessage(responseMessage);
    }

    @Nullable
    private static String reason(final Object obj) {
      if (!(obj instanceof Message msg)) {
        return null;
      }
      // iterate through all fields on the message
      for (Map.Entry<Descriptors.FieldDescriptor, Object> field : msg.getAllFields().entrySet()) {
        // iterate through all options on the field
        for (Map.Entry<Descriptors.FieldDescriptor, Object> option : field.getKey().getOptions().getAllFields().entrySet()) {
          if (option.getKey().getFullName().equals("org.signal.chat.tag.reason")) {
            if (!(option.getValue() instanceof String s)) {
              log.error("Invalid value for option tag.reason {}", option.getValue());
              continue;
            }
            // return the first tag we see
            return s;
          }
        }

        // No reason on this field. Recursively check subfields of this field for a reason
        final String subReason = reason(field.getValue());
        if (subReason != null) {
          return subReason;
        }
      }
      // No field or subfield contained an annotated reason
      return null;
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

  private static Optional<ErrorInfo> errorInfo(final com.google.rpc.Status statusProto) {
    return statusProto.getDetailsList().stream()
        .filter(any -> any.is(ErrorInfo.class))
        .map(errorInfo -> {
          try {
            return errorInfo.unpack(ErrorInfo.class);
          } catch (final InvalidProtocolBufferException e) {
            throw new UncheckedIOException(e);
          }
        })
        .findFirst();
  }
}
