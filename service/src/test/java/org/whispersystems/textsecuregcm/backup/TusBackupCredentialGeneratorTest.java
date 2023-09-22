/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.backup;

import org.apache.commons.lang3.RandomUtils;
import org.junit.jupiter.api.Test;
import org.whispersystems.textsecuregcm.attachments.TusConfiguration;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretBytes;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class TusBackupCredentialGeneratorTest {
  @Test
  public void uploadGenerator() {
    TusBackupCredentialGenerator generator = new TusBackupCredentialGenerator(new TusConfiguration(
        new SecretBytes(RandomUtils.nextBytes(32)),
        "https://example.org/upload"));

    final MessageBackupUploadDescriptor messageBackupUploadDescriptor = generator.generateUpload("subdir", "key");
    assertThat(messageBackupUploadDescriptor.signedUploadLocation()).isEqualTo("https://example.org/upload/backups");
    assertThat(messageBackupUploadDescriptor.key()).isEqualTo("subdir/key");
    assertThat(messageBackupUploadDescriptor.headers()).containsKey("Authorization");
    final String username = parseUsername(messageBackupUploadDescriptor.headers().get("Authorization"));
    assertThat(username).isEqualTo("write$backups/subdir/key");
  }

  @Test
  public void readCredential() {
    TusBackupCredentialGenerator generator = new TusBackupCredentialGenerator(new TusConfiguration(
        new SecretBytes(RandomUtils.nextBytes(32)),
        "https://example.org/upload"));

    final Map<String, String> headers = generator.readHeaders("subdir");
    assertThat(headers).containsKey("Authorization");
    final String username = parseUsername(headers.get("Authorization"));
    assertThat(username).isEqualTo("read$backups/subdir");
  }

  private static String parseUsername(final String authHeader) {
    assertThat(authHeader).startsWith("Basic");
    final String encoded = authHeader.substring("Basic".length() + 1);
    final String cred = new String(Base64.getDecoder().decode(encoded), StandardCharsets.UTF_8);
    return cred.split(":")[0];
  }
}
