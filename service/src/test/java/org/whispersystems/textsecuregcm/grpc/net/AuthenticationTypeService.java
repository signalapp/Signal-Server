package org.whispersystems.textsecuregcm.grpc.net;

import io.grpc.stub.StreamObserver;
import org.signal.chat.rpc.AuthenticationTypeGrpc;
import org.signal.chat.rpc.GetAuthenticatedRequest;
import org.signal.chat.rpc.GetAuthenticatedResponse;

public class AuthenticationTypeService extends AuthenticationTypeGrpc.AuthenticationTypeImplBase {

  private final boolean authenticated;

  public AuthenticationTypeService(final boolean authenticated) {
    this.authenticated = authenticated;
  }

  @Override
  public void getAuthenticated(final GetAuthenticatedRequest request, final StreamObserver<GetAuthenticatedResponse> responseObserver) {
    responseObserver.onNext(GetAuthenticatedResponse.newBuilder().setAuthenticated(authenticated).build());
    responseObserver.onCompleted();
  }
}
