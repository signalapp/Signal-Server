/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import org.signal.chat.calling.quality.SimpleCallQualityGrpc;
import org.signal.chat.calling.quality.SubmitCallQualitySurveyRequest;
import org.signal.chat.calling.quality.SubmitCallQualitySurveyResponse;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.CallQualityInvalidArgumentsException;
import org.whispersystems.textsecuregcm.metrics.CallQualitySurveyManager;

public class CallQualitySurveyGrpcService extends SimpleCallQualityGrpc.CallQualityImplBase {

  private final CallQualitySurveyManager callQualitySurveyManager;
  private final RateLimiters rateLimiters;

  public CallQualitySurveyGrpcService(final CallQualitySurveyManager callQualitySurveyManager,
      final RateLimiters rateLimiters) {

    this.callQualitySurveyManager = callQualitySurveyManager;
    this.rateLimiters = rateLimiters;
  }

  @Override
  public SubmitCallQualitySurveyResponse submitCallQualitySurvey(final SubmitCallQualitySurveyRequest request)
      throws RateLimitExceededException {

    final String remoteAddress = RequestAttributesUtil.getRemoteAddress().getHostAddress();

    rateLimiters.getSubmitCallQualitySurveyLimiter().validate(remoteAddress);

    try {
      callQualitySurveyManager.submitCallQualitySurvey(request,
          remoteAddress,
          RequestAttributesUtil.getUserAgent().orElse(null));
    } catch (final CallQualityInvalidArgumentsException e) {
      throw e.getField()
          .map(fieldName -> GrpcExceptions.fieldViolation(fieldName, e.getMessage()))
          .orElseGet(() -> GrpcExceptions.invalidArguments(e.getMessage()));
    }

    return SubmitCallQualitySurveyResponse.getDefaultInstance();
  }
}
