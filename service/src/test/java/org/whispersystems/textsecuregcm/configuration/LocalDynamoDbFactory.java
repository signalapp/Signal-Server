/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
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

  /**
   * If true, tables will be created the first time a DynamoDB client is built.
   * <p>
   * Defaults to {@code true}.
   */
  @JsonProperty
  boolean initTables = true;

  public LocalDynamoDbFactory() {
    try {
      EXTENSION.beforeAll(null);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      try {
        EXTENSION.close();
      } catch (Throwable e) {
        throw new RuntimeException(e);
      }
    }));
  }

  @Override
  public DynamoDbClient buildSyncClient(final AwsCredentialsProvider awsCredentialsProvider, final MetricPublisher metricPublisher) {
    initTablesIfNecessary();
    return EXTENSION.getDynamoDbClient();
  }

  @Override
  public DynamoDbAsyncClient buildAsyncClient(final AwsCredentialsProvider awsCredentialsProvider, final MetricPublisher metricPublisher) {
    initTablesIfNecessary();
    return EXTENSION.getDynamoDbAsyncClient();
  }

  private void initTablesIfNecessary() {
    try {
      if (initTables) {
        EXTENSION.beforeEach(null);
        initTables = false;
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
