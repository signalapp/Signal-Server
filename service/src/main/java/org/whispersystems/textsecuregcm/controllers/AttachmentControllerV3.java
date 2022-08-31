/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Base64;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.entities.AttachmentDescriptorV3;
import org.whispersystems.textsecuregcm.gcp.CanonicalRequest;
import org.whispersystems.textsecuregcm.gcp.CanonicalRequestGenerator;
import org.whispersystems.textsecuregcm.gcp.CanonicalRequestSigner;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;

@Path("/v3/attachments")
public class AttachmentControllerV3 {

  @Nonnull
  private final RateLimiter rateLimiter;

  @Nonnull
  private final CanonicalRequestGenerator canonicalRequestGenerator;

  @Nonnull
  private final CanonicalRequestSigner canonicalRequestSigner;

  @Nonnull
  private final SecureRandom secureRandom;

  public AttachmentControllerV3(@Nonnull RateLimiters rateLimiters, @Nonnull String domain, @Nonnull String email,
      int maxSizeInBytes, @Nonnull String pathPrefix, @Nonnull String rsaSigningKey)
      throws IOException, InvalidKeyException, InvalidKeySpecException {
    this.rateLimiter = rateLimiters.getAttachmentLimiter();
    this.canonicalRequestGenerator = new CanonicalRequestGenerator(domain, email, maxSizeInBytes, pathPrefix);
    this.canonicalRequestSigner = new CanonicalRequestSigner(rsaSigningKey);
    this.secureRandom = new SecureRandom();
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/form/upload")
  public AttachmentDescriptorV3 getAttachmentUploadForm(@Auth AuthenticatedAccount auth)
      throws RateLimitExceededException {
    rateLimiter.validate(auth.getAccount().getUuid());

    final ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    final String key = generateAttachmentKey();
    final CanonicalRequest canonicalRequest = canonicalRequestGenerator.createFor(key, now);

    return new AttachmentDescriptorV3(2, key, getHeaderMap(canonicalRequest),
        getSignedUploadLocation(canonicalRequest));
  }

  private String getSignedUploadLocation(@Nonnull CanonicalRequest canonicalRequest) {
    return "https://" + canonicalRequest.getDomain() + canonicalRequest.getResourcePath()
            + '?' + canonicalRequest.getCanonicalQuery()
            + "&X-Goog-Signature=" + canonicalRequestSigner.sign(canonicalRequest);
  }

  private static Map<String, String> getHeaderMap(@Nonnull CanonicalRequest canonicalRequest) {
    return Map.of(
      "host", canonicalRequest.getDomain(),
      "x-goog-content-length-range", "1," + canonicalRequest.getMaxSizeInBytes(),
      "x-goog-resumable", "start");
  }

  private String generateAttachmentKey() {
    final byte[] bytes = new byte[15];
    secureRandom.nextBytes(bytes);
    return Base64.getUrlEncoder().encodeToString(bytes);
  }
}
