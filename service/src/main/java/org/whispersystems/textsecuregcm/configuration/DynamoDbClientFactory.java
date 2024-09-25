/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.dropwizard.jackson.Discoverable;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = DynamoDbClientConfiguration.class)
public interface DynamoDbClientFactory extends Discoverable {

  DynamoDbClient buildSyncClient(AwsCredentialsProvider awsCredentialsProvider, MetricPublisher metricPublisher);

  DynamoDbAsyncClient buildAsyncClient(AwsCredentialsProvider awsCredentialsProvider, MetricPublisher metricPublisher);
}
