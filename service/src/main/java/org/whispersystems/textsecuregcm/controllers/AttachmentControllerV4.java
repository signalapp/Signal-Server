/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.whispersystems.textsecuregcm.attachments.AttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.GcsAttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.TusAttachmentGenerator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.entities.AttachmentDescriptorV3;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.websocket.auth.ReadOnly;


/**
 * The V4 API is identical to the {@link AttachmentControllerV3} API, but supports an additional TUS based cdn type (cdn3)
 */
@Path("/v4/attachments")
@Tag(name = "Attachments")
public class AttachmentControllerV4 {

  public static final String CDN3_EXPERIMENT_NAME = "cdn3";

  private final ExperimentEnrollmentManager experimentEnrollmentManager;
  private final RateLimiter rateLimiter;

  private final Map<Integer, AttachmentGenerator> attachmentGenerators;

  @Nonnull
  private final SecureRandom secureRandom;

  public AttachmentControllerV4(
      final RateLimiters rateLimiters,
      final GcsAttachmentGenerator gcsAttachmentGenerator,
      final TusAttachmentGenerator tusAttachmentGenerator,
      final ExperimentEnrollmentManager experimentEnrollmentManager) {
    this.rateLimiter = rateLimiters.getAttachmentLimiter();
    this.experimentEnrollmentManager = experimentEnrollmentManager;
    this.secureRandom = new SecureRandom();
    this.attachmentGenerators = Map.of(
        2, gcsAttachmentGenerator,
        3, tusAttachmentGenerator
    );
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/form/upload")
  @Operation(
      summary = "Get an upload form",
      description = """
          Retrieve an upload form that can be used to perform a resumable upload. The response will include a cdn number
          indicating what protocol should be used to perform the upload.
          """
  )
  @ApiResponse(responseCode = "200", description = "Success, response body includes upload form", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "413", description = "Too many attempts", headers = @Header(
      name = "Retry-After",
      description = "If present, an positive integer indicating the number of seconds before a subsequent attempt could succeed"))
  @ApiResponse(responseCode = "429", description = "Too many attempts", headers = @Header(
      name = "Retry-After",
      description = "If present, an positive integer indicating the number of seconds before a subsequent attempt could succeed"))
  public AttachmentDescriptorV3 getAttachmentUploadForm(@ReadOnly @Auth AuthenticatedAccount auth)
      throws RateLimitExceededException {
    rateLimiter.validate(auth.getAccount().getUuid());
    final String key = generateAttachmentKey();
    final boolean useCdn3 = this.experimentEnrollmentManager.isEnrolled(auth.getAccount().getUuid(), CDN3_EXPERIMENT_NAME);
    int cdn = useCdn3 ? 3 : 2;
    final AttachmentGenerator.Descriptor descriptor = this.attachmentGenerators.get(cdn).generateAttachment(key);
    return new AttachmentDescriptorV3(cdn, key, descriptor.headers(), descriptor.signedUploadLocation());
  }

  private String generateAttachmentKey() {
    final byte[] bytes = new byte[15];
    secureRandom.nextBytes(bytes);
    return Base64.getUrlEncoder().encodeToString(bytes);
  }
}

