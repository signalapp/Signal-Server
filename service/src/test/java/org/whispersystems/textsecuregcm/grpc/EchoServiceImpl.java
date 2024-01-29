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
  public void echo(EchoRequest req, StreamObserver<EchoResponse> responseObserver) {
    responseObserver.onNext(EchoResponse.newBuilder().setPayload(req.getPayload()).build());
    responseObserver.onCompleted();
  }

  @Override
  public void echo2(EchoRequest req, StreamObserver<EchoResponse> responseObserver) {
    responseObserver.onNext(EchoResponse.newBuilder().setPayload(req.getPayload()).build());
    responseObserver.onCompleted();
  }
}
