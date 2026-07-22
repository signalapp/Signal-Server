/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.grpc.Status;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.signal.chat.calling.CallingGrpc;
import org.signal.chat.calling.GetCallingRelaysRequest;
import org.signal.chat.calling.GetCallingRelaysResponse;
import org.whispersystems.textsecuregcm.auth.CloudflareTurnCredentialsManager;
import org.whispersystems.textsecuregcm.auth.TurnToken;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;

class CallingGrpcServiceTest extends SimpleBaseGrpcTest<CallingGrpcService, CallingGrpc.CallingBlockingStub> {

  @Mock
  private CloudflareTurnCredentialsManager cloudflareTurnCredentialsManager;

  @Mock
  private RateLimiter rateLimiter;

  @Override
  protected CallingGrpcService createServiceBeforeEachTest() {
    final RateLimiters rateLimiters = mock(RateLimiters.class);
    when(rateLimiters.getCallEndpointLimiter()).thenReturn(rateLimiter);

    return new CallingGrpcService(cloudflareTurnCredentialsManager, rateLimiters);
  }

  @Test
  void getCallingRelays() throws IOException {
    final String username = "username";
    final String password = "password";
    final long credentialTtlSeconds = 73;
    final String hostnameTurnUrl = "turn:example.com:443";
    final String ipTurnUrl = "turn:127.0.0.1:80";
    final String ipTurnsUrl = "turns:127.0.0.1:443";
    final String hostname = "example.com";

    when(cloudflareTurnCredentialsManager.retrieveFromCloudflare(any()))
        .thenReturn(new TurnToken(username,
            password,
            credentialTtlSeconds,
            List.of(hostnameTurnUrl),
            List.of(ipTurnUrl, ipTurnsUrl),
            hostname));

    assertEquals(GetCallingRelaysResponse.newBuilder()
        .addRelays(GetCallingRelaysResponse.Relay.newBuilder()
            .setUsername(username)
            .setPassword(password)
            .setCredentialTtlSeconds(credentialTtlSeconds)
            .setHostnameUrls(GetCallingRelaysResponse.HostnameUrlList.newBuilder()
                .addUrls(hostnameTurnUrl)
                .build())
            .setIpUrls(GetCallingRelaysResponse.IpUrlList.newBuilder()
                .addUrls(ipTurnUrl)
                .addUrls(ipTurnsUrl)
                .setHostname(hostname)
                .build())
            .build())
        .build(),
        authenticatedServiceStub().getCallingRelays(GetCallingRelaysRequest.getDefaultInstance()));
  }

  @Test
  void getCallingRelaysIoException() throws IOException {
    when(cloudflareTurnCredentialsManager.retrieveFromCloudflare(any()))
        .thenThrow(IOException.class);

    GrpcTestUtils.assertStatusException(Status.UNAVAILABLE,
        () -> authenticatedServiceStub().getCallingRelays(GetCallingRelaysRequest.getDefaultInstance()));
  }

  @Test
  void getCallingRelaysRateLimited() throws RateLimitExceededException {
    final Duration retryAfter = Duration.ofSeconds(19);

    doThrow(new RateLimitExceededException(retryAfter))
        .when(rateLimiter).validate(any(UUID.class));

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertRateLimitExceeded(retryAfter,
        () -> authenticatedServiceStub().getCallingRelays(GetCallingRelaysRequest.getDefaultInstance()));
  }
}
