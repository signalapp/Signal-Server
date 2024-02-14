/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import io.dropwizard.auth.Auth;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Base64;
import javax.annotation.Nonnull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.whispersystems.textsecuregcm.attachments.AttachmentGenerator;
import org.whispersystems.textsecuregcm.attachments.GcsAttachmentGenerator;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.entities.AttachmentDescriptorV3;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.websocket.auth.ReadOnly;

@Path("/v3/attachments")
@Tag(name = "Attachments")
public class AttachmentControllerV3 {

  @Nonnull
  private final RateLimiter rateLimiter;

  @Nonnull
  private final GcsAttachmentGenerator gcsAttachmentGenerator;

  @Nonnull
  private final SecureRandom secureRandom;

  public AttachmentControllerV3(@Nonnull RateLimiters rateLimiters, @Nonnull GcsAttachmentGenerator gcsAttachmentGenerator)
      throws IOException, InvalidKeyException, InvalidKeySpecException {
    this.rateLimiter = rateLimiters.getAttachmentLimiter();
    this.gcsAttachmentGenerator = gcsAttachmentGenerator;
    this.secureRandom = new SecureRandom();
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/form/upload")
  public AttachmentDescriptorV3 getAttachmentUploadForm(@ReadOnly @Auth AuthenticatedAccount auth)
      throws RateLimitExceededException {
    rateLimiter.validate(auth.getAccount().getUuid());
    final String key = generateAttachmentKey();
    final AttachmentGenerator.Descriptor descriptor = this.gcsAttachmentGenerator.generateAttachment(key);
    return new AttachmentDescriptorV3(2, key, descriptor.headers(), descriptor.signedUploadLocation());
  }

  private String generateAttachmentKey() {
    final byte[] bytes = new byte[15];
    secureRandom.nextBytes(bytes);
    return Base64.getUrlEncoder().encodeToString(bytes);
  }
}
