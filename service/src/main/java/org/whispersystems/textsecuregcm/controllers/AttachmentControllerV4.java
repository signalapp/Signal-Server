/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.headers.Header;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.tags.Tag;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Positive;
import jakarta.ws.rs.ClientErrorException;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.QueryParam;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.security.SecureRandom;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.whispersystems.textsecuregcm.attachments.AttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.AttachmentUtil;
import org.whispersystems.textsecuregcm.attachments.GcsAttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.TusAttachmentGenerator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedDevice;
import org.whispersystems.textsecuregcm.entities.AttachmentDescriptorV3;
import org.whispersystems.textsecuregcm.experiment.ExperimentEnrollmentManager;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;


/**
 * The attachment controller generates "upload forms" for authenticated users that permit them to upload files
 * (message attachments) to a remote storage location. The location may be selected by the server at runtime.
 */
@Path("/v4/attachments")
@Tag(name = "Attachments")
public class AttachmentControllerV4 {

  private final ExperimentEnrollmentManager experimentEnrollmentManager;
  private final RateLimiter countRateLimiter;
  private final RateLimiter bytesRateLimiter;
  private final long maxUploadLength;

  private final Map<Integer, AttachmentGenerator> attachmentGenerators;

  @Nonnull
  private final SecureRandom secureRandom;

  public AttachmentControllerV4(
      final RateLimiters rateLimiters,
      final GcsAttachmentGenerator gcsAttachmentGenerator,
      final TusAttachmentGenerator tusAttachmentGenerator,
      final ExperimentEnrollmentManager experimentEnrollmentManager,
      final long maxUploadLength) {
    this.countRateLimiter = rateLimiters.getAttachmentLimiter();
    this.bytesRateLimiter = rateLimiters.getAttachmentBytesLimiter();
    this.experimentEnrollmentManager = experimentEnrollmentManager;
    this.maxUploadLength = maxUploadLength;
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

          Uploads with the returned form will be limited to a maximum size of the provided uploadLength.
          """
  )
  @ApiResponse(responseCode = "200", description = "Success, response body includes upload form", useReturnTypeSchema = true)
  @ApiResponse(responseCode = "400", description = "The provided uploadLength was not valid")
  @ApiResponse(responseCode = "413", description = "The provided uploadLength is larger than the maximum supported upload size. The maximum upload size is subject to change and is governed by `global.attachments.maxBytes`.")
  @ApiResponse(responseCode = "429", description = "Too many attempts", headers = @Header(
      name = "Retry-After",
      description = "If present, an positive integer indicating the number of seconds before a subsequent attempt could succeed"))
  public AttachmentDescriptorV3 getAttachmentUploadForm(
      @Auth AuthenticatedDevice auth,
      @Parameter(description = "The size of the attachment to upload in bytes")
      @QueryParam("uploadLength") final @Valid Optional<@Positive Long> maybeUploadLength)
      throws RateLimitExceededException {

    final long uploadLength = maybeUploadLength.orElse(maxUploadLength);
    if (uploadLength > maxUploadLength) {
      throw new ClientErrorException("exceeded maximum uploadLength", Response.Status.REQUEST_ENTITY_TOO_LARGE);
    }

    countRateLimiter.validate(auth.accountIdentifier());
    if (maybeUploadLength.isPresent()) {
      // Ideally we'd check these two rate limits transactionally and only update them if both permits were acquired.
      // However, just undoing the first modification if the second one fails is close enough for our purposes
      try {
        bytesRateLimiter.validate(auth.accountIdentifier(), maybeUploadLength.get());
      } catch (RateLimitExceededException e) {
        countRateLimiter.restorePermits(auth.accountIdentifier(), 1);
        throw e;
      }
    }

    final String key = AttachmentUtil.generateAttachmentKey(secureRandom);
    final boolean useCdn3 = this.experimentEnrollmentManager.isEnrolled(auth.accountIdentifier(), AttachmentUtil.CDN3_EXPERIMENT_NAME);
    int cdn = useCdn3 ? 3 : 2;
    final AttachmentGenerator.Descriptor descriptor = this.attachmentGenerators.get(cdn).generateAttachment(key, uploadLength);
    return new AttachmentDescriptorV3(cdn, key, descriptor.headers(), descriptor.signedUploadLocation());
  }

}
