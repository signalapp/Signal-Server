/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.grpc.net.GrpcClientConnectionManager;

class ChannelShutdownInterceptorTest {

  private GrpcClientConnectionManager grpcClientConnectionManager;
  private ChannelShutdownInterceptor channelShutdownInterceptor;

  private ServerCallHandler<String, String> nextCallHandler;

  private static final Metadata HEADERS = new Metadata();

  @BeforeEach
  void setUp() {
    grpcClientConnectionManager = mock(GrpcClientConnectionManager.class);
    channelShutdownInterceptor = new ChannelShutdownInterceptor(grpcClientConnectionManager);

    //noinspection unchecked
    nextCallHandler = mock(ServerCallHandler.class);

    //noinspection unchecked
    when(nextCallHandler.startCall(any(), any())).thenReturn(mock(ServerCall.Listener.class));
  }

  @Test
  void interceptCallComplete() {
    @SuppressWarnings("unchecked") final ServerCall<String, String> serverCall = mock(ServerCall.class);

    when(grpcClientConnectionManager.handleServerCallStart(serverCall)).thenReturn(true);

    final ServerCall.Listener<String> serverCallListener =
        channelShutdownInterceptor.interceptCall(serverCall, HEADERS, nextCallHandler);

    serverCallListener.onComplete();

    verify(grpcClientConnectionManager).handleServerCallStart(serverCall);
    verify(grpcClientConnectionManager).handleServerCallComplete(serverCall);
    verify(serverCall, never()).close(any(), any());
  }

  @Test
  void interceptCallCancelled() {
    @SuppressWarnings("unchecked") final ServerCall<String, String> serverCall = mock(ServerCall.class);

    when(grpcClientConnectionManager.handleServerCallStart(serverCall)).thenReturn(true);

    final ServerCall.Listener<String> serverCallListener =
        channelShutdownInterceptor.interceptCall(serverCall, HEADERS, nextCallHandler);

    serverCallListener.onCancel();

    verify(grpcClientConnectionManager).handleServerCallStart(serverCall);
    verify(grpcClientConnectionManager).handleServerCallComplete(serverCall);
    verify(serverCall, never()).close(any(), any());
  }

  @Test
  void interceptCallChannelClosing() {
    @SuppressWarnings("unchecked") final ServerCall<String, String> serverCall = mock(ServerCall.class);

    when(grpcClientConnectionManager.handleServerCallStart(serverCall)).thenReturn(false);

    channelShutdownInterceptor.interceptCall(serverCall, HEADERS, nextCallHandler);

    verify(grpcClientConnectionManager).handleServerCallStart(serverCall);
    verify(grpcClientConnectionManager, never()).handleServerCallComplete(serverCall);
    verify(serverCall).close(eq(Status.UNAVAILABLE), any());
  }
}
