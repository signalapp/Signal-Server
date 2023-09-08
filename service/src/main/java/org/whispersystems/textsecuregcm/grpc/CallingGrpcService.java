/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import org.signal.chat.calling.GetTurnCredentialsRequest;
import org.signal.chat.calling.GetTurnCredentialsResponse;
import org.signal.chat.calling.ReactorCallingGrpc;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.auth.grpc.AuthenticationUtil;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import reactor.core.publisher.Mono;

public class CallingGrpcService extends ReactorCallingGrpc.CallingImplBase {

  private final TurnTokenGenerator turnTokenGenerator;
  private final RateLimiters rateLimiters;

  public CallingGrpcService(final TurnTokenGenerator turnTokenGenerator, final RateLimiters rateLimiters) {
    this.turnTokenGenerator = turnTokenGenerator;
    this.rateLimiters = rateLimiters;
  }

  @Override
  public Mono<GetTurnCredentialsResponse> getTurnCredentials(final GetTurnCredentialsRequest request) {
    final AuthenticatedDevice authenticatedDevice = AuthenticationUtil.requireAuthenticatedDevice();

    return rateLimiters.getTurnLimiter().validateReactive(authenticatedDevice.accountIdentifier())
        .then(Mono.fromSupplier(() -> turnTokenGenerator.generate(authenticatedDevice.accountIdentifier())))
        .map(turnToken -> GetTurnCredentialsResponse.newBuilder()
            .setUsername(turnToken.username())
            .setPassword(turnToken.password())
            .addAllUrls(turnToken.urls())
            .build());
  }
}
