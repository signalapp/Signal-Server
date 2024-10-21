/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.s3;

import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import org.whispersystems.textsecuregcm.util.Pair;

public class PostPolicyGenerator {

  public static final DateTimeFormatter AWS_DATE_TIME = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssX");
  private static final DateTimeFormatter CREDENTIAL_DATE = DateTimeFormatter.ofPattern("yyyyMMdd");

  private final String region;
  private final String bucket;
  private final String awsAccessId;

  public PostPolicyGenerator(final String region, final String bucket, final String awsAccessId) {
    this.region = region;
    this.bucket = bucket;
    this.awsAccessId = awsAccessId;
  }

  public Pair<String, String> createFor(final ZonedDateTime now, final String object, final int maxSizeInBytes) {
    final String expiration = now.plusMinutes(30).format(DateTimeFormatter.ISO_INSTANT);
    final String credentialDate = now.format(CREDENTIAL_DATE);
    final String requestDate = now.format(AWS_DATE_TIME);
    final String credential = String.format("%s/%s/%s/s3/aws4_request", awsAccessId, credentialDate, region);

    final String policy = String.format("""
        {
          "expiration": "%s",
          "conditions": [
            {"bucket": "%s"},
            {"key": "%s"},
            {"acl": "private"},
            ["starts-with", "$Content-Type", ""],
            ["content-length-range", 1, %d],

            {"x-amz-credential": "%s"},
            {"x-amz-algorithm": "AWS4-HMAC-SHA256"},
            {"x-amz-date": "%s" }
          ]
        }
        """, expiration, bucket, object, maxSizeInBytes, credential, requestDate);

    return new Pair<>(credential, Base64.getEncoder().encodeToString(policy.getBytes(StandardCharsets.UTF_8)));
  }
}
