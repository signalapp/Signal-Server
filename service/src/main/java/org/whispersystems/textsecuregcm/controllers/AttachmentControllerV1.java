/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import com.amazonaws.HttpMethod;
import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import java.net.URL;
import java.util.stream.Stream;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.entities.AttachmentDescriptorV1;
import org.whispersystems.textsecuregcm.entities.AttachmentUri;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.s3.UrlSigner;


@Path("/v1/attachments")
public class AttachmentControllerV1 extends AttachmentControllerBase {

  @SuppressWarnings("unused")
  private final Logger logger = LoggerFactory.getLogger(AttachmentControllerV1.class);

  private static final String[] UNACCELERATED_REGIONS = {"+20", "+971", "+968", "+974"};

  private final RateLimiters rateLimiters;
  private final UrlSigner urlSigner;

  public AttachmentControllerV1(RateLimiters rateLimiters, String accessKey, String accessSecret, String bucket) {
    this.rateLimiters = rateLimiters;
    this.urlSigner = new UrlSigner(accessKey, accessSecret, bucket);
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public AttachmentDescriptorV1 allocateAttachment(@Auth AuthenticatedAccount auth) throws RateLimitExceededException {

    rateLimiters.getAttachmentLimiter().validate(auth.getAccount().getUuid());

    long attachmentId = generateAttachmentId();
    URL url = urlSigner.getPreSignedUrl(attachmentId, HttpMethod.PUT,
        Stream.of(UNACCELERATED_REGIONS).anyMatch(region -> auth.getAccount().getNumber().startsWith(region)));

    return new AttachmentDescriptorV1(attachmentId, url.toExternalForm());
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/{attachmentId}")
  public AttachmentUri redirectToAttachment(@Auth AuthenticatedAccount auth,
      @PathParam("attachmentId") long attachmentId) {
    return new AttachmentUri(urlSigner.getPreSignedUrl(attachmentId, HttpMethod.GET,
        Stream.of(UNACCELERATED_REGIONS).anyMatch(region -> auth.getAccount().getNumber().startsWith(region))));
  }

}
