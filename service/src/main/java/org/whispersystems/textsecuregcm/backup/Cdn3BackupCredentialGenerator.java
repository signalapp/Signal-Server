/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import org.apache.http.HttpHeaders;
import org.whispersystems.textsecuregcm.attachments.TusConfiguration;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentials;
import org.whispersystems.textsecuregcm.auth.ExternalServiceCredentialsGenerator;
import org.whispersystems.textsecuregcm.util.HeaderUtils;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Base64;
import java.util.Map;

public class Cdn3BackupCredentialGenerator {

  public static final String CDN_PATH = "backups";
  public static final int BACKUP_CDN = 3;

  private static String READ_PERMISSION = "read";
  private static String WRITE_PERMISSION = "write";
  private static String PERMISSION_SEPARATOR = "$";

  // Write entities will be of the form 'write$backups/<string>
  private static final String WRITE_ENTITY_PREFIX = String.format("%s%s%s/", WRITE_PERMISSION, PERMISSION_SEPARATOR,
      CDN_PATH);
  // Read entities will be of the form 'read$backups/<string>
  private static final String READ_ENTITY_PREFIX = String.format("%s%s%s/", READ_PERMISSION, PERMISSION_SEPARATOR,
      CDN_PATH);

  private final ExternalServiceCredentialsGenerator credentialsGenerator;
  private final String tusUri;

  public Cdn3BackupCredentialGenerator(final TusConfiguration cfg) {
    this.tusUri = cfg.uploadUri();
    this.credentialsGenerator = credentialsGenerator(Clock.systemUTC(), cfg);
  }

  private static ExternalServiceCredentialsGenerator credentialsGenerator(final Clock clock,
      final TusConfiguration cfg) {
    return ExternalServiceCredentialsGenerator
        .builder(cfg.userAuthenticationTokenSharedSecret())
        .prependUsername(false)
        .withClock(clock)
        .build();
  }

  public BackupUploadDescriptor generateUpload(final String key) {
    if (key.isBlank()) {
      throw new IllegalArgumentException("Upload descriptors must have non-empty keys");
    }
    final String entity = WRITE_ENTITY_PREFIX + key;
    final ExternalServiceCredentials credentials = credentialsGenerator.generateFor(entity);
    final String b64Key = Base64.getEncoder().encodeToString(key.getBytes(StandardCharsets.UTF_8));
    final Map<String, String> headers = Map.of(
        HttpHeaders.AUTHORIZATION, HeaderUtils.basicAuthHeader(credentials),
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
    final ExternalServiceCredentials credentials = credentialsGenerator.generateFor(
        READ_ENTITY_PREFIX + hashedBackupId);
    return Map.of(HttpHeaders.AUTHORIZATION, HeaderUtils.basicAuthHeader(credentials));
  }
}
