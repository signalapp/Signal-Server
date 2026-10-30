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
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicGrpcCompressionConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

/// Requests gzip compression of responses for configured gRPC methods. Compression is only applied if the client
/// advertises gzip in its `grpc-accept-encoding` header; otherwise gRPC falls back to sending uncompressed responses.
public class GrpcCompressionInterceptor implements ServerInterceptor {

  private static final String GZIP = "gzip";

  private final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager;

  public GrpcCompressionInterceptor(
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(final ServerCall<ReqT, RespT> serverCall,
      final Metadata metadata, final ServerCallHandler<ReqT, RespT> next) {
    final DynamicGrpcCompressionConfiguration compressionConfiguration =
        this.dynamicConfigurationManager.getConfiguration().getGrpcCompression();
    final MethodDescriptor<ReqT, RespT> methodDescriptor = serverCall.getMethodDescriptor();
    if (compressionConfiguration.compressedServices().contains(methodDescriptor.getServiceName()) ||
        compressionConfiguration.compressedMethods().contains(methodDescriptor.getFullMethodName())) {
      // This does not necessarily enable compression. The implementation will handle negotiation with the client
      // and may fall back to no compression.
      serverCall.setCompression(GZIP);
    }
    return next.startCall(serverCall, metadata);
  }
}
