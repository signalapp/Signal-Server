/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeName;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;

@JsonTypeName("default")
public record DefaultAwsCredentialsFactory() implements AwsCredentialsProviderFactory {

  // FLT(uoemai): Allow for more flexibility in providing AWS credentials.
  public AwsCredentialsProvider build() {
    return DefaultCredentialsProvider.create();
  }
}
