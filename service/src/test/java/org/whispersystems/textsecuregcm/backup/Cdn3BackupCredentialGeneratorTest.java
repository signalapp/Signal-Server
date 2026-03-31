/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.attachments.TusConfiguration;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;
import org.whispersystems.textsecuregcm.util.TestRandomUtil;

public class Cdn3BackupCredentialGeneratorTest {
  private static final long MAX_UPLOAD_LENGTH = 100;
  private static final byte[] SECRET = TestRandomUtil.nextBytes(32);

  @Test
  public void uploadGenerator() {
    final Cdn3BackupCredentialGenerator generator = new Cdn3BackupCredentialGenerator(new TusConfiguration(
        new SecretBytes(SECRET),
        "https://example.org/upload"));

    final BackupUploadDescriptor messageBackupUploadDescriptor = generator.generateUpload("subdir/key", MAX_UPLOAD_LENGTH);
    assertThat(messageBackupUploadDescriptor.signedUploadLocation()).isEqualTo("https://example.org/upload/backups");
    assertThat(messageBackupUploadDescriptor.key()).isEqualTo("subdir/key");
    assertThat(messageBackupUploadDescriptor.headers()).containsKey("Authorization");
    final String token = parseBearerToken(messageBackupUploadDescriptor.headers().get("Authorization"));

    final DecodedJWT decoded = JWT.require(Algorithm.HMAC256(SECRET))
        .withAudience(Cdn3BackupCredentialGenerator.CDN_PATH)
        .withSubject("subdir/key")
        .withClaim("scope", "write")
        .withClaim("maxLen", MAX_UPLOAD_LENGTH)
        .build().verify(token);
    assertThat(decoded.getClaims()).containsOnlyKeys("aud", "sub", "scope", "iat", "maxLen");
  }

  @Test
  public void readCredential() {
    final Cdn3BackupCredentialGenerator generator = new Cdn3BackupCredentialGenerator(new TusConfiguration(
        new SecretBytes(SECRET),
        "https://example.org/upload"));

    final Map<String, String> headers = generator.readHeaders("subdir");
    assertThat(headers).containsKey("Authorization");
    final DecodedJWT decoded = JWT.require(Algorithm.HMAC256(SECRET))
        .withAudience(Cdn3BackupCredentialGenerator.CDN_PATH)
        .withSubject("subdir")
        .withClaim("scope", "read")
        .build().verify(parseBearerToken(headers.get("Authorization")));
    assertThat(decoded.getClaims()).containsOnlyKeys("aud", "sub", "scope", "iat");
  }

  private static String parseBearerToken(final String authHeader) {
    assertThat(authHeader).startsWith("Bearer");
    return authHeader.substring("Bearer".length() + 1);
  }
}
