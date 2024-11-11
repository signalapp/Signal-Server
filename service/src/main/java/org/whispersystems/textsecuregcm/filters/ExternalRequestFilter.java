/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.filters;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.micrometer.core.instrument.Metrics;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.grpc.RequestAttributesUtil;
import org.whispersystems.textsecuregcm.util.InetAddressRange;

public class ExternalRequestFilter implements Filter, ServerInterceptor {

  private static final Logger logger = LoggerFactory.getLogger(ExternalRequestFilter.class);

  private static final String REQUESTS_COUNTER_NAME = name(ExternalRequestFilter.class, "requests");
  private static final String PROTOCOL_TAG_NAME = "protocol";
  private static final String BLOCKED_TAG_NAME = "blocked";

  private final Set<InetAddressRange> permittedInternalAddressRanges;
  private final Set<String> filteredGrpcMethodNames;

  public ExternalRequestFilter(final Set<InetAddressRange> permittedInternalAddressRanges,
      final Set<String> filteredGrpcMethodNames) {
      this.permittedInternalAddressRanges = permittedInternalAddressRanges;
    this.filteredGrpcMethodNames = filteredGrpcMethodNames;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> call,
      final Metadata headers, final ServerCallHandler<ReqT, RespT> next) {

    final MethodDescriptor<ReqT, RespT> methodDescriptor = call.getMethodDescriptor();
    final boolean shouldFilterMethod = filteredGrpcMethodNames.contains(methodDescriptor.getFullMethodName());

    final InetAddress remoteAddress = RequestAttributesUtil.getRemoteAddress();
    final boolean blocked = shouldFilterMethod && shouldBlock(remoteAddress);

    Metrics.counter(REQUESTS_COUNTER_NAME,
            PROTOCOL_TAG_NAME, "grpc",
            BLOCKED_TAG_NAME, String.valueOf(blocked))
        .increment();

    if (blocked) {
      call.close(Status.NOT_FOUND, new Metadata());
      return new ServerCall.Listener<>() {};
    }

    return next.startCall(call, headers);
  }

  @Override
  public void doFilter(final ServletRequest request, final ServletResponse response, final FilterChain chain)
      throws IOException, ServletException {

    final InetAddress remoteInetAddress = InetAddress.getByName(
        (String) request.getAttribute(RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME));
    final boolean restricted = shouldBlock(remoteInetAddress);

    Metrics.counter(REQUESTS_COUNTER_NAME,
            PROTOCOL_TAG_NAME, "http",
            BLOCKED_TAG_NAME, String.valueOf(restricted))
        .increment();

    if (restricted) {
      if (response instanceof HttpServletResponse hsr) {
        hsr.setStatus(404);
      } else {
        logger.warn("response was an unexpected type: {}", response.getClass());
      }
      return;
    }

    chain.doFilter(request, response);
  }

  public boolean shouldBlock(InetAddress remoteAddress) {
    return permittedInternalAddressRanges.stream()
        .noneMatch(range -> range.contains(remoteAddress));
  }

}
