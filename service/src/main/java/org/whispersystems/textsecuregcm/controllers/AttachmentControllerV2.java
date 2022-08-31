/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import com.codahale.metrics.annotation.Timed;
import io.dropwizard.auth.Auth;
import java.security.SecureRandom;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.entities.AttachmentDescriptorV2;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.s3.PolicySigner;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.util.Conversions;
import org.whispersystems.textsecuregcm.util.Pair;

@Path("/v2/attachments")
public class AttachmentControllerV2 {

  private final PostPolicyGenerator policyGenerator;
  private final PolicySigner policySigner;
  private final RateLimiter rateLimiter;

  public AttachmentControllerV2(RateLimiters rateLimiters, String accessKey, String accessSecret, String region,
      String bucket) {
    this.rateLimiter = rateLimiters.getAttachmentLimiter();
    this.policyGenerator = new PostPolicyGenerator(region, bucket, accessKey);
    this.policySigner = new PolicySigner(accessSecret, region);
  }

  @Timed
  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/form/upload")
  public AttachmentDescriptorV2 getAttachmentUploadForm(@Auth AuthenticatedAccount auth)
      throws RateLimitExceededException {
    rateLimiter.validate(auth.getAccount().getUuid());

    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    long attachmentId = generateAttachmentId();
    String objectName = String.valueOf(attachmentId);
    Pair<String, String> policy = policyGenerator.createFor(now, String.valueOf(objectName), 100 * 1024 * 1024);
    String signature = policySigner.getSignature(now, policy.second());

    return new AttachmentDescriptorV2(attachmentId, objectName, policy.first(),
        "private", "AWS4-HMAC-SHA256",
        now.format(PostPolicyGenerator.AWS_DATE_TIME),
        policy.second(), signature);
  }

  private long generateAttachmentId() {
    byte[] attachmentBytes = new byte[8];
    new SecureRandom().nextBytes(attachmentBytes);

    attachmentBytes[0] = (byte) (attachmentBytes[0] & 0x7F);
    return Conversions.byteArrayToLong(attachmentBytes);
  }

}
