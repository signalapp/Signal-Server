/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import jakarta.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.configuration.secrets.SecretString;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = StaticAwsCredentialsFactory.class)
@JsonTypeName("static")
public record StaticAwsCredentialsFactory(@NotNull SecretString accessKeyId,
                                          @NotNull SecretString secretAccessKey) implements
    AwsCredentialsProviderFactory {

  @Override
  public AwsCredentialsProvider build() {
    return StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId.value(), secretAccessKey.value()));
  }
}
