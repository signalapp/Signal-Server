/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.whispersystems.textsecuregcm.grpc.GrpcTestUtils.assertRateLimitExceeded;

import java.time.Duration;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.signal.chat.calling.CallingGrpc;
import org.signal.chat.calling.GetTurnCredentialsRequest;
import org.signal.chat.calling.GetTurnCredentialsResponse;
import org.whispersystems.textsecuregcm.auth.TurnToken;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.util.MockUtils;

class CallingGrpcServiceTest extends SimpleBaseGrpcTest<CallingGrpcService, CallingGrpc.CallingBlockingStub> {

  @Mock
  private TurnTokenGenerator turnTokenGenerator;

  @Mock
  private RateLimiter turnCredentialRateLimiter;


  @Override
  protected CallingGrpcService createServiceBeforeEachTest() {
    final RateLimiters rateLimiters = mock(RateLimiters.class);
    when(rateLimiters.getTurnLimiter()).thenReturn(turnCredentialRateLimiter);
    return new CallingGrpcService(turnTokenGenerator, rateLimiters);
  }

  @Test
  void getTurnCredentials() {
    final String username = "test-username";
    final String password = "test-password";
    final List<String> urls = List.of("first", "second");

    MockUtils.updateRateLimiterResponseToAllow(turnCredentialRateLimiter, AUTHENTICATED_ACI);
    when(turnTokenGenerator.generate(any())).thenReturn(new TurnToken(username, password, urls));

    final GetTurnCredentialsResponse response = authenticatedServiceStub().getTurnCredentials(GetTurnCredentialsRequest.newBuilder().build());

    final GetTurnCredentialsResponse expectedResponse = GetTurnCredentialsResponse.newBuilder()
        .setUsername(username)
        .setPassword(password)
        .addAllUrls(urls)
        .build();

    assertEquals(expectedResponse, response);
  }

  @Test
  void getTurnCredentialsRateLimited() {
    final Duration retryAfter = MockUtils.updateRateLimiterResponseToFail(
        turnCredentialRateLimiter, AUTHENTICATED_ACI, Duration.ofMinutes(19), false);
    assertRateLimitExceeded(retryAfter, () -> authenticatedServiceStub().getTurnCredentials(GetTurnCredentialsRequest.newBuilder().build()));
    verify(turnTokenGenerator, never()).generate(any());
    verifyNoInteractions(turnTokenGenerator);
  }
}
