package org.whispersystems.textsecuregcm.util;

import org.whispersystems.textsecuregcm.configuration.DynamoDbClientConfiguration;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

public class DynamoDbFromConfig {

  public static DynamoDbClient client(DynamoDbClientConfiguration config, AwsCredentialsProvider credentialsProvider) {
    AwsCredentialsProvider newCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("AKIA5AJJCNFKZFHORBNQ", "nNPXns15+zm/qs1fNOhkKOdQghToxDMZKiXDwUi0"));   
    return DynamoDbClient.builder()
        .region(Region.of(config.getRegion()))
        .credentialsProvider(newCredentialsProvider)
        .overrideConfiguration(ClientOverrideConfiguration.builder()
            .apiCallTimeout(config.getClientExecutionTimeout())
            .apiCallAttemptTimeout(config.getClientRequestTimeout())
            .build())
        .build();
  }

  public static DynamoDbAsyncClient asyncClient(
      DynamoDbClientConfiguration config,
      AwsCredentialsProvider credentialsProvider) {
    AwsCredentialsProvider newCredentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create("AKIA5AJJCNFKZFHORBNQ", "nNPXns15+zm/qs1fNOhkKOdQghToxDMZKiXDwUi0"));    
    return DynamoDbAsyncClient.builder()
        .region(Region.of(config.getRegion()))
        .credentialsProvider(newCredentialsProvider)
        .overrideConfiguration(ClientOverrideConfiguration.builder()
            .apiCallTimeout(config.getClientExecutionTimeout())
            .apiCallAttemptTimeout(config.getClientRequestTimeout())
            .build())
        .build();
  }
}
