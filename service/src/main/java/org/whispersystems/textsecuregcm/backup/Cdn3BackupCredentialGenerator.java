/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import org.apache.http.HttpHeaders;
import org.whispersystems.textsecuregcm.attachments.TusConfiguration;
import org.whispersystems.textsecuregcm.auth.JwtGenerator;
import org.whispersystems.textsecuregcm.util.HeaderUtils;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Base64;
import java.util.Map;

public class Cdn3BackupCredentialGenerator {
  public static final String CDN_PATH = "backups";
  public static final int BACKUP_CDN = 3;

  private final String tusUri;
  private final JwtGenerator jwtGenerator;
  private final long maxUploadSize;

  public Cdn3BackupCredentialGenerator(final TusConfiguration cfg) {
    this.tusUri = cfg.uploadUri();
    this.jwtGenerator = new JwtGenerator(cfg.userAuthenticationTokenSharedSecret().value(), Clock.systemUTC());
    this.maxUploadSize = cfg.maxSizeInBytes();
  }

  public BackupUploadDescriptor generateUpload(final String key) {
    if (key.isBlank()) {
      throw new IllegalArgumentException("Upload descriptors must have non-empty keys");
    }

    final String token = jwtGenerator.generateJwt(CDN_PATH, key, builder -> builder
        .withClaim(JwtGenerator.MAX_LENGTH_CLAIM_KEY, maxUploadSize)
        .withClaim(JwtGenerator.SCOPE_CLAIM_KEY, "write"));

    final String b64Key = Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8));
    final Map<String, String> headers = Map.of(
        HttpHeaders.AUTHORIZATION, HeaderUtils.bearerAuthHeader(token),
        "Upload-Metadata", String.format("filename %s", b64Key));

    return new BackupUploadDescriptor(
        BACKUP_CDN,
        key,
        headers,
        tusUri + "/" + CDN_PATH);
  }

  public Map<String, String> readHeaders(final String hashedBackupId) {
    if (hashedBackupId.isBlank()) {
      throw new IllegalArgumentException("Backup subdir name must be non-empty");
    }

    final String token = jwtGenerator.generateJwt(CDN_PATH, hashedBackupId, builder -> builder
        .withClaim(JwtGenerator.SCOPE_CLAIM_KEY, "read"));
    return Map.of(HttpHeaders.AUTHORIZATION, HeaderUtils.bearerAuthHeader(token));
  }
}
