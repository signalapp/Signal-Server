/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import java.util.Optional;
import javax.annotation.Nullable;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@JsonTypeName("local")
public class LocalDynamoDbFactory implements DynamoDbClientFactory {

  private static final DynamoDbExtension EXTENSION = new DynamoDbExtension(DynamoDbExtensionSchema.Tables.values());

  private boolean initExtension = true;

  /**
   * If true, tables will be created the first time a DynamoDB client is built.
   * <p>
   * Defaults to {@code true}.
   */
  @JsonProperty
  boolean initTables = true;

  /**
   * If specified, will be provided to {@link DynamoDbExtension} to use instead of its embedded container
   */
  @Nullable
  @JsonProperty
  DynamoDbLocalOverrides overrides;

  @Override
  public DynamoDbClient buildSyncClient(final AwsCredentialsProvider awsCredentialsProvider, final MetricPublisher metricPublisher) {
    initExtensionIfNecessary();
    initTablesIfNecessary();

    return EXTENSION.getDynamoDbClient();
  }

  @Override
  public DynamoDbAsyncClient buildAsyncClient(final AwsCredentialsProvider awsCredentialsProvider, final MetricPublisher metricPublisher) {
    initExtensionIfNecessary();
    initTablesIfNecessary();

    return EXTENSION.getDynamoDbAsyncClient();
  }

  private void initExtensionIfNecessary() {
    if (initExtension) {
      try {
        Optional.ofNullable(overrides)
            .ifPresent(o -> {
              Optional.ofNullable(o.endpoint).ifPresent(EXTENSION::setEndpointOverride);
              Optional.ofNullable(o.region).ifPresent(EXTENSION::setRegion);
              Optional.ofNullable(o.awsCredentialsProvider).ifPresent(p -> EXTENSION.setAwsCredentialsProvider(p.build()));
            });

        EXTENSION.beforeAll(null);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
          try {
            EXTENSION.close();
          } catch (Throwable e) {
            throw new RuntimeException(e);
          }
        }));

        initExtension = false;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
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

  private record DynamoDbLocalOverrides(@Nullable String endpoint, @Nullable AwsCredentialsProviderFactory awsCredentialsProvider, @Nullable String region) {}
}
