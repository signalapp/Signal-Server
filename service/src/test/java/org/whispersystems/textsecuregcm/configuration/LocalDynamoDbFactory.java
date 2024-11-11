/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeName;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@JsonTypeName("local")
public class LocalDynamoDbFactory implements DynamoDbClientFactory {

  private static final DynamoDbExtension EXTENSION = new DynamoDbExtension(DynamoDbExtensionSchema.Tables.values());

  static {
    try {
      EXTENSION.beforeEach(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    Runtime.getRuntime().addShutdownHook(new Thread(() -> EXTENSION.afterEach(null)));
  }

  @Override
  public DynamoDbClient buildSyncClient(final AwsCredentialsProvider awsCredentialsProvider, final MetricPublisher metricPublisher) {
    return EXTENSION.getDynamoDbClient();
  }

  @Override
  public DynamoDbAsyncClient buildAsyncClient(final AwsCredentialsProvider awsCredentialsProvider, final MetricPublisher metricPublisher) {
    return EXTENSION.getDynamoDbAsyncClient();
  }
}
