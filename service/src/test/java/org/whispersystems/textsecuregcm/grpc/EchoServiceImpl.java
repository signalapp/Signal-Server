/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.stub.StreamObserver;
import org.signal.chat.rpc.EchoRequest;
import org.signal.chat.rpc.EchoResponse;
import org.signal.chat.rpc.EchoServiceGrpc;

public class EchoServiceImpl extends EchoServiceGrpc.EchoServiceImplBase {
  @Override
  public void echo(final EchoRequest echoRequest, final StreamObserver<EchoResponse> responseObserver) {
    responseObserver.onNext(buildResponse(echoRequest));
    responseObserver.onCompleted();
  }

  @Override
  public void echo2(final EchoRequest echoRequest, final StreamObserver<EchoResponse> responseObserver) {
    responseObserver.onNext(buildResponse(echoRequest));
    responseObserver.onCompleted();
  }

  @Override
  public StreamObserver<EchoRequest> echoStream(final StreamObserver<EchoResponse> responseObserver) {
    return new StreamObserver<>() {
      @Override
      public void onNext(final EchoRequest echoRequest) {
        responseObserver.onNext(buildResponse(echoRequest));
      }

      @Override
      public void onError(final Throwable throwable) {
        responseObserver.onError(throwable);
      }

      @Override
      public void onCompleted() {
        responseObserver.onCompleted();
      }
    };
  }

  private static EchoResponse buildResponse(final EchoRequest echoRequest) {
    return EchoResponse.newBuilder().setPayload(echoRequest.getPayload()).build();
  }
}
