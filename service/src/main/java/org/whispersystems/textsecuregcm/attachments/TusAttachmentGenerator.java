/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.attachments;

import org.apache.http.HttpHeaders;
import org.whispersystems.textsecuregcm.auth.JwtGenerator;
import org.whispersystems.textsecuregcm.util.HeaderUtils;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Base64;
import java.util.Map;

public class TusAttachmentGenerator implements AttachmentGenerator {

  private static final String ATTACHMENTS = "attachments";

  private final JwtGenerator jwtGenerator;
  private final String tusUri;
  private final long maxUploadSize;

  public TusAttachmentGenerator(final TusConfiguration cfg) {
    this.tusUri = cfg.uploadUri();
    this.jwtGenerator = new JwtGenerator(cfg.userAuthenticationTokenSharedSecret().value(), Clock.systemUTC());
    this.maxUploadSize = cfg.maxSizeInBytes();
  }

  @Override
  public Descriptor generateAttachment(final String key, final long uploadLength) {
    if (uploadLength > maxUploadSize) {
      throw new IllegalArgumentException("uploadLength " + uploadLength + " exceeds maximum " + maxUploadSize);
    }

    final String token = jwtGenerator.generateJwt(ATTACHMENTS, key,
        builder -> builder.withClaim(JwtGenerator.MAX_LENGTH_CLAIM_KEY, uploadLength));
    final String b64Key = Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8));

    final Map<String, String> headers = Map.of(
        HttpHeaders.AUTHORIZATION, HeaderUtils.bearerAuthHeader(token),
        "Upload-Metadata", String.format("filename %s", b64Key)
    );
    return new Descriptor(headers, tusUri + "/" +  ATTACHMENTS);
  }

  @Override
  public long maxUploadSizeInBytes() {
    return maxUploadSize;
  }
}
