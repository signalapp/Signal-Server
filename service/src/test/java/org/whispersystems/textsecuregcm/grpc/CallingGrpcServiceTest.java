/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.grpc.ServerInterceptors;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.signal.chat.calling.CallingGrpc;
import org.signal.chat.calling.GetTurnCredentialsRequest;
import org.signal.chat.calling.GetTurnCredentialsResponse;
import org.whispersystems.textsecuregcm.auth.TurnToken;
import org.whispersystems.textsecuregcm.auth.TurnTokenGenerator;
import org.whispersystems.textsecuregcm.auth.grpc.MockAuthenticationInterceptor;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Device;
import reactor.core.publisher.Mono;

class CallingGrpcServiceTest {

  private TurnTokenGenerator turnTokenGenerator;
  private RateLimiter turnCredentialRateLimiter;

  private CallingGrpc.CallingBlockingStub callingStub;

  private static final UUID AUTHENTICATED_ACI = UUID.randomUUID();
  private static final long AUTHENTICATED_DEVICE_ID = Device.MASTER_ID;

  @RegisterExtension
  static final GrpcServerExtension GRPC_SERVER_EXTENSION = new GrpcServerExtension();

  @BeforeEach
  void setUp() {
    turnTokenGenerator = mock(TurnTokenGenerator.class);
    turnCredentialRateLimiter = mock(RateLimiter.class);

    final RateLimiters rateLimiters = mock(RateLimiters.class);
    when(rateLimiters.getTurnLimiter()).thenReturn(turnCredentialRateLimiter);

    final CallingGrpcService callingGrpcService = new CallingGrpcService(turnTokenGenerator, rateLimiters);

    final MockAuthenticationInterceptor mockAuthenticationInterceptor = new MockAuthenticationInterceptor();
    mockAuthenticationInterceptor.setAuthenticatedDevice(AUTHENTICATED_ACI, AUTHENTICATED_DEVICE_ID);

    GRPC_SERVER_EXTENSION.getServiceRegistry()
        .addService(ServerInterceptors.intercept(callingGrpcService, mockAuthenticationInterceptor));

    callingStub = CallingGrpc.newBlockingStub(GRPC_SERVER_EXTENSION.getChannel());
  }

  @Test
  void getTurnCredentials() {
    final String username = "test-username";
    final String password = "test-password";
    final List<String> urls = List.of("first", "second");

    when(turnCredentialRateLimiter.validateReactive(AUTHENTICATED_ACI)).thenReturn(Mono.empty());
    when(turnTokenGenerator.generate(any())).thenReturn(new TurnToken(username, password, urls));

    final GetTurnCredentialsResponse response = callingStub.getTurnCredentials(GetTurnCredentialsRequest.newBuilder().build());

    final GetTurnCredentialsResponse expectedResponse = GetTurnCredentialsResponse.newBuilder()
        .setUsername(username)
        .setPassword(password)
        .addAllUrls(urls)
        .build();

    assertEquals(expectedResponse, response);
  }

  @Test
  void getTurnCredentialsRateLimited() {
    final Duration retryAfter = Duration.ofMinutes(19);

    when(turnCredentialRateLimiter.validateReactive(AUTHENTICATED_ACI))
        .thenReturn(Mono.error(new RateLimitExceededException(retryAfter, false)));

    final StatusRuntimeException exception =
        assertThrows(StatusRuntimeException.class, () -> callingStub.getTurnCredentials(GetTurnCredentialsRequest.newBuilder().build()));

    verify(turnTokenGenerator, never()).generate(any());

    assertEquals(Status.Code.RESOURCE_EXHAUSTED, exception.getStatus().getCode());
    assertNotNull(exception.getTrailers());
    assertEquals(retryAfter, exception.getTrailers().get(RateLimitUtil.RETRY_AFTER_DURATION_KEY));

    verifyNoInteractions(turnTokenGenerator);
  }
}
