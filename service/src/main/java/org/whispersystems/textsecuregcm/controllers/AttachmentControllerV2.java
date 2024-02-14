/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.net.HttpHeaders;
import io.dropwizard.auth.Auth;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.security.SecureRandom;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.whispersystems.textsecuregcm.auth.AuthenticatedAccount;
import org.whispersystems.textsecuregcm.entities.AttachmentDescriptorV2;
import org.whispersystems.textsecuregcm.limits.RateLimiter;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.metrics.UserAgentTagUtil;
import org.whispersystems.textsecuregcm.s3.PolicySigner;
import org.whispersystems.textsecuregcm.s3.PostPolicyGenerator;
import org.whispersystems.textsecuregcm.util.Conversions;
import org.whispersystems.textsecuregcm.util.Pair;
import org.whispersystems.websocket.auth.ReadOnly;

@Path("/v2/attachments")
@Tag(name = "Attachments")
public class AttachmentControllerV2 {

  private final PostPolicyGenerator policyGenerator;
  private final PolicySigner policySigner;
  private final RateLimiter rateLimiter;

  private static final String CREATE_UPLOAD_COUNTER_NAME = name(AttachmentControllerV2.class, "uploadForm");

  public AttachmentControllerV2(RateLimiters rateLimiters, String accessKey, String accessSecret, String region,
      String bucket) {
    this.rateLimiter = rateLimiters.getAttachmentLimiter();  
    this.policyGenerator = new PostPolicyGenerator(region, bucket, accessKey);
    this.policySigner = new PolicySigner(accessSecret, region);
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  @Path("/form/upload")
  public AttachmentDescriptorV2 getAttachmentUploadForm(
      @ReadOnly @Auth AuthenticatedAccount auth,
      @HeaderParam(HttpHeaders.USER_AGENT) String userAgent)
      throws RateLimitExceededException {
    rateLimiter.validate(auth.getAccount().getUuid());

    ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
    long attachmentId = generateAttachmentId();
    String objectName = String.valueOf(attachmentId);
    Pair<String, String> policy = policyGenerator.createFor(now, objectName, 100 * 1024 * 1024);
    String signature = policySigner.getSignature(now, policy.second());

    Metrics.counter(CREATE_UPLOAD_COUNTER_NAME, Tags.of(UserAgentTagUtil.getPlatformTag(userAgent))).increment();

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
