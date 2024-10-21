/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.s3;

import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HexFormat;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

public class PolicySigner {

  private final String awsAccessSecret;
  private final String region;

  private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd");

  public PolicySigner(final String awsAccessSecret, final String region) {
    this.awsAccessSecret = awsAccessSecret;
    this.region = region;
  }

  // See https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_sigv-create-signed-request.html
  public String getSignature(final ZonedDateTime now, final String policy) {
    final Mac mac;

    try {
      mac = Mac.getInstance("HmacSHA256");
    } catch (final NoSuchAlgorithmException e) {
      throw new AssertionError("Every implementation of the Java platform is required to support HmacSHA256", e);
    }

    try {
      mac.init(toHmacKey(("AWS4" + awsAccessSecret).getBytes(StandardCharsets.UTF_8)));
      final byte[] dateKey = mac.doFinal(now.format(DATE_FORMAT).getBytes(StandardCharsets.UTF_8));

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
