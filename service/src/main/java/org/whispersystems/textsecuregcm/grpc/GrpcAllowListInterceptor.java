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
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicGrpcAllowListConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

public class GrpcAllowListInterceptor implements ServerInterceptor {

  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;


  public GrpcAllowListInterceptor(
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall,
      final Metadata metadata, final ServerCallHandler<ReqT, RespT> next) {
    final DynamicGrpcAllowListConfiguration allowList = this.dynamicConfigurationManager.getConfiguration().getGrpcAllowList();
    final MethodDescriptor<ReqT, RespT> methodDescriptor = serverCall.getMethodDescriptor();
    if (!allowList.enableAll() &&
        !allowList.enabledServices().contains(methodDescriptor.getServiceName()) &&
        !allowList.enabledMethods().contains(methodDescriptor.getFullMethodName())) {
      return ServerInterceptorUtil.closeWithStatus(serverCall, Status.UNIMPLEMENTED);
    }
    return next.startCall(serverCall, metadata);
  }
}
