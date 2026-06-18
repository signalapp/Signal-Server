/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.s3;

import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class PostPolicyGenerator {

  public static final String ALGORITHM = "AWS4-HMAC-SHA256";
  public static final String ACL = "private";

  private static final DateTimeFormatter AWS_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssX");
  private static final DateTimeFormatter AWS_DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

  private final String region;
  private final String bucket;
  private final String awsAccessId;
  private final String awsAccessSecret;

  public record SignedPostPolicy(String formattedTimestamp, String credential, String encodedPolicy, String signature) {
  }

  public PostPolicyGenerator(final String region,
      final String bucket,
      final String awsAccessId,
      final String awsAccessSecret) {

    this.region = region;
    this.bucket = bucket;
    this.awsAccessId = awsAccessId;
    this.awsAccessSecret = awsAccessSecret;
  }

  public SignedPostPolicy createFor(final String object, final int maxSizeInBytes, final Instant currentTime) {
    final ZonedDateTime now = ZonedDateTime.ofInstant(currentTime, ZoneOffset.UTC);
    final String expiration = now.plusMinutes(30).format(DateTimeFormatter.ISO_INSTANT);
    final String credentialDate = now.format(AWS_DATE_FORMATTER);
    final String requestTimestamp = now.format(AWS_TIMESTAMP_FORMATTER);
    final String credential = String.format("%s/%s/%s/s3/aws4_request", awsAccessId, credentialDate, region);

    final String policy = String.format("""
        {
          "expiration": "%s",
          "conditions": [
            {"bucket": "%s"},
            {"key": "%s"},
            {"acl": "%s"},
            ["starts-with", "$Content-Type", ""],
            ["content-length-range", 1, %d],

            {"x-amz-credential": "%s"},
            {"x-amz-algorithm": "%s"},
            {"x-amz-date": "%s" }
          ]
        }
        """, expiration, bucket, object, ACL, maxSizeInBytes, credential, ALGORITHM, requestTimestamp);

    final String encodedPolicy = Base64.getEncoder().encodeToString(policy.getBytes(StandardCharsets.UTF_8));

    return new SignedPostPolicy(requestTimestamp, credential, encodedPolicy, getSignature(now, encodedPolicy));
  }

  // See https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html
  @VisibleForTesting
  String getSignature(final ZonedDateTime now, final String policy) {
    final Mac mac;

    try {
      mac = Mac.getInstance("HmacSHA256");
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError("Every implementation of the Java platform is required to support HmacSHA256", e);
    }

    try {
      mac.init(toHmacKey(("AWS4" + awsAccessSecret).getBytes(StandardCharsets.UTF_8)));
      final byte[] dateKey = mac.doFinal(now.format(AWS_DATE_FORMATTER).getBytes(StandardCharsets.UTF_8));

      mac.init(toHmacKey(dateKey));
      final byte[] dateRegionKey = mac.doFinal(region.getBytes(StandardCharsets.UTF_8));

      mac.init(toHmacKey(dateRegionKey));
      final byte[] dateRegionServiceKey = mac.doFinal("s3".getBytes(StandardCharsets.UTF_8));

      mac.init(toHmacKey(dateRegionServiceKey));
      final byte[] signingKey = mac.doFinal("aws4_request".getBytes(StandardCharsets.UTF_8));

      mac.init(toHmacKey(signingKey));

      return HexFormat.of().formatHex(mac.doFinal(policy.getBytes(StandardCharsets.UTF_8)));
    } catch (final InvalidKeyException e) {
      throw new AssertionError(e);
    }
  }

  private static Key toHmacKey(final byte[] bytes) {
    return new SecretKeySpec(bytes, "HmacSHA256");
  }
}
