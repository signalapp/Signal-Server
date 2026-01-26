/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.net.InetAddresses;
import java.time.Duration;
import io.grpc.Status;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.signal.chat.calling.quality.CallQualityGrpc;
import org.signal.chat.calling.quality.SubmitCallQualitySurveyRequest;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.CallQualityInvalidArgumentsException;
import org.whispersystems.textsecuregcm.metrics.CallQualitySurveyManager;

class CallQualitySurveyGrpcServiceTest extends SimpleBaseGrpcTest<CallQualitySurveyGrpcService, CallQualityGrpc.CallQualityBlockingStub> {

  @Mock
  private CallQualitySurveyManager callQualitySurveyManager;

  @Mock
  private RateLimiter rateLimiter;

  private static final String USER_AGENT = "Signal-iOS/7.78.0.1041 iOS/18.3.2 libsignal/0.80.3";
  private static final String REMOTE_ADDRESS = "127.0.0.1";

  @BeforeEach
  void setUp() {
    getMockRequestAttributesInterceptor()
        .setRequestAttributes(new RequestAttributes(InetAddresses.forString(REMOTE_ADDRESS), USER_AGENT, null));
  }

  @Override
  protected CallQualitySurveyGrpcService createServiceBeforeEachTest() {
    final RateLimiters rateLimiters = mock(RateLimiters.class);
    when(rateLimiters.getSubmitCallQualitySurveyLimiter()).thenReturn(rateLimiter);

    return new CallQualitySurveyGrpcService(callQualitySurveyManager, rateLimiters);
  }

  @Test
  void submitCallQualitySurvey() throws CallQualityInvalidArgumentsException {
    final SubmitCallQualitySurveyRequest request = SubmitCallQualitySurveyRequest.getDefaultInstance();
    assertDoesNotThrow(() -> unauthenticatedServiceStub().submitCallQualitySurvey(request));

    verify(callQualitySurveyManager).submitCallQualitySurvey(request, REMOTE_ADDRESS, USER_AGENT);
  }

  @Test
  void submitCallQualitySurveyRateLimited() throws RateLimitExceededException {
    final Duration retryAfter = Duration.ofMinutes(17);

    doThrow(new RateLimitExceededException(retryAfter))
        .when(rateLimiter).validate(REMOTE_ADDRESS);

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertRateLimitExceeded(retryAfter,
        () -> unauthenticatedServiceStub().submitCallQualitySurvey(SubmitCallQualitySurveyRequest.getDefaultInstance()));
  }

  @Test
  void submitCallQualitySurveyInvalidArgument() throws CallQualityInvalidArgumentsException {
    final SubmitCallQualitySurveyRequest request = SubmitCallQualitySurveyRequest.getDefaultInstance();

    doThrow(new CallQualityInvalidArgumentsException("test"))
        .when(callQualitySurveyManager).submitCallQualitySurvey(request, REMOTE_ADDRESS, USER_AGENT);

    //noinspection ResultOfMethodCallIgnored
    GrpcTestUtils.assertStatusException(Status.INVALID_ARGUMENT,
        () -> unauthenticatedServiceStub().submitCallQualitySurvey(request));
  }
}
