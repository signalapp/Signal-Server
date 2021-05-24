/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import com.almworks.sqlite4java.SQLite;
import com.amazonaws.services.dynamodbv2.local.main.ServerRunner;
import com.amazonaws.services.dynamodbv2.local.server.DynamoDBProxyServer;
import org.junit.rules.ExternalResource;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.net.ServerSocket;
import java.net.URI;

public class LocalDynamoDbRule extends ExternalResource {
  private DynamoDBProxyServer server;
  private int port;

  @Override
  protected void before() throws Throwable {
    super.before();
    SQLite.setLibraryPath("target/lib");  // if you see a library failed to load error, you need to run mvn test-compile at least once first
    ServerSocket serverSocket = new ServerSocket(0);
    serverSocket.setReuseAddress(false);
    port = serverSocket.getLocalPort();
    serverSocket.close();
    server = ServerRunner.createServerFromCommandLineArgs(new String[]{"-inMemory", "-port", String.valueOf(port)});
    server.start();
  }

  @Override
  protected void after() {
    try {
      server.stop();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    super.after();
  }

  public DynamoDbClient getDynamoDbClient() {
    return DynamoDbClient.builder()
        .endpointOverride(URI.create("http://localhost:" + port))
        .region(Region.of("local-test-region"))
        .credentialsProvider(StaticCredentialsProvider.create(
            AwsBasicCredentials.create("accessKey", "secretKey")))
        .build();
  }
}
