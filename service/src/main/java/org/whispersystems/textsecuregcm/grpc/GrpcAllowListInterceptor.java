/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GrpcAllowListInterceptor implements ServerInterceptor {

  private final boolean enableAll;
  private final Set<String> enabledServices;
  private final Set<String> enabledMethods;


  public GrpcAllowListInterceptor(
      final boolean enableAll,
      final List<String> enabledServices,
      final List<String> enabledMethods) {
    this.enableAll = enableAll;
    this.enabledServices = new HashSet<>(enabledServices);
    this.enabledMethods = new HashSet<>(enabledMethods);
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall,
      final Metadata metadata, final ServerCallHandler<ReqT, RespT> next) {
    final MethodDescriptor<ReqT, RespT> methodDescriptor = serverCall.getMethodDescriptor();
    if (!enableAll && !enabledServices.contains(methodDescriptor.getServiceName()) && !enabledMethods.contains(methodDescriptor.getFullMethodName())) {
      return ServerInterceptorUtil.closeWithStatus(serverCall, Status.UNIMPLEMENTED);
    }
    return next.startCall(serverCall, metadata);
  }
}
