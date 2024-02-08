package org.whispersystems.textsecuregcm.util;

import org.whispersystems.textsecuregcm.configuration.DynamoDbClientConfiguration;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.http.crt.AwsCrtHttpClient;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDbFromConfig {

  public static DynamoDbClient client(DynamoDbClientConfiguration config, AwsCredentialsProvider credentialsProvider) {
    return DynamoDbClient.builder()
        .region(Region.of(config.region()))
        .credentialsProvider(credentialsProvider)
        .overrideConfiguration(ClientOverrideConfiguration.builder()
            .apiCallTimeout(config.clientExecutionTimeout())
            .apiCallAttemptTimeout(config.clientRequestTimeout())
            .build())
        .httpClientBuilder(AwsCrtHttpClient
            .builder()
            .maxConcurrency(config.maxConnections()))
        .build();
  }

  public static DynamoDbAsyncClient asyncClient(
      DynamoDbClientConfiguration config,
      AwsCredentialsProvider credentialsProvider) {
    return DynamoDbAsyncClient.builder()
        .region(Region.of(config.region()))
        .credentialsProvider(credentialsProvider)
        .overrideConfiguration(ClientOverrideConfiguration.builder()
            .apiCallTimeout(config.clientExecutionTimeout())
            .apiCallAttemptTimeout(config.clientRequestTimeout())
            .build())
        .httpClientBuilder(NettyNioAsyncHttpClient.builder()
            .maxConcurrency(config.maxConnections()))
        .build();
  }
}
