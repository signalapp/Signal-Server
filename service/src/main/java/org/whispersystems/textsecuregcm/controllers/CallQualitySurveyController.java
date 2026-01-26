/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.google.common.net.HttpHeaders;
import com.google.protobuf.InvalidProtocolBufferException;
import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.parameters.RequestBody;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.ForbiddenException;
import jakarta.ws.rs.HeaderParam;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.WebApplicationException;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import java.util.Optional;
import org.signal.chat.calling.quality.SubmitCallQualitySurveyRequest;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.filters.RemoteAddressFilter;
import org.whispersystems.textsecuregcm.limits.RateLimitedByIp;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.CallQualityInvalidArgumentsException;
import org.whispersystems.textsecuregcm.metrics.CallQualitySurveyManager;

@Path("/v1/call_quality_survey")
@io.swagger.v3.oas.annotations.tags.Tag(name = "Call quality survey")
public class CallQualitySurveyController {

  private final CallQualitySurveyManager callQualitySurveyManager;

  public CallQualitySurveyController(final CallQualitySurveyManager callQualitySurveyManager) {
    this.callQualitySurveyManager = callQualitySurveyManager;
  }

  @PUT
  @Consumes(MediaType.APPLICATION_OCTET_STREAM)
  @Produces(MediaType.APPLICATION_JSON)
  @Operation(summary = "Submit survey response", description = "Submits a call quality survey response")
  @ApiResponse(responseCode = "204", description = "The survey response was submitted successfully")
  @ApiResponse(responseCode = "422", description = "The survey response could not be parsed")
  @ApiResponse(responseCode = "429", description = "Too many attempts", headers = @Header(
      name = "Retry-After",
      description = "If present, an positive integer indicating the number of seconds before a subsequent attempt could succeed"))
  @RateLimitedByIp(RateLimiters.For.SUBMIT_CALL_QUALITY_SURVEY)
  public void submitCallQualitySurvey(@Auth final Optional<AuthenticatedDevice> authenticatedDevice,
      @RequestBody(description = "A serialized survey response protobuf entity")
      @NotNull final byte[] surveyResponse,
      @HeaderParam(HttpHeaders.USER_AGENT) final String userAgentString,
      @Context final ContainerRequestContext requestContext) {

    if (authenticatedDevice.isPresent()) {
      throw new ForbiddenException("must not use authenticated connection for call quality survey submissions");
    }

    final SubmitCallQualitySurveyRequest submitCallQualitySurveyRequest;

    try {
      submitCallQualitySurveyRequest = SubmitCallQualitySurveyRequest.parseFrom(surveyResponse);
    } catch (final InvalidProtocolBufferException e) {
      throw new WebApplicationException("Invalid protobuf entity", 422);
    }

    final String remoteAddress = (String) requestContext.getProperty(RemoteAddressFilter.REMOTE_ADDRESS_ATTRIBUTE_NAME);

    try {
      callQualitySurveyManager.submitCallQualitySurvey(submitCallQualitySurveyRequest, remoteAddress, userAgentString);
    } catch (final CallQualityInvalidArgumentsException e) {
      throw new WebApplicationException(e.getMessage(), 422);
    }
  }
}
