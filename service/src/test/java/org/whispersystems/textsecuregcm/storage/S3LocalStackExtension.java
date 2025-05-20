/*
 * Copyright 2021-2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;

import java.util.Objects;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;

@Testcontainers
public class S3LocalStackExtension implements BeforeEachCallback, AfterEachCallback, BeforeAllCallback,
    AfterAllCallback {

  private final static DockerImageName LOCAL_STACK_IMAGE =
      DockerImageName.parse(Objects.requireNonNull(
          System.getProperty("localstackImage"),
          "Local stack image not found; must provide localstackImage system property"));

  private static LocalStackContainer LOCAL_STACK = new LocalStackContainer(LOCAL_STACK_IMAGE).withServices(S3);

  private final String bucketName;
  private S3AsyncClient s3Client;

  public S3LocalStackExtension(final String bucketName) {
    this.bucketName = bucketName;
  }

  @Override
  public void afterEach(ExtensionContext context) {
    Flux.from(s3Client.listObjectsV2Paginator(ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build())
            .contents())
        .flatMap(obj -> Mono.fromFuture(() -> s3Client.deleteObject(DeleteObjectRequest.builder()
            .bucket(bucketName)
            .key(obj.key())
            .build())), 100)
        .then()
        .block();
    s3Client.deleteBucket(DeleteBucketRequest.builder().bucket(bucketName).build()).join();
  }


  @Override
  public void beforeEach(ExtensionContext context) throws Exception {
    s3Client.createBucket(CreateBucketRequest.builder().bucket(bucketName).build()).join();
  }

  public S3AsyncClient getS3Client() {
    return s3Client;
  }

  @Override
  public void afterAll(final ExtensionContext context) throws Exception {
    s3Client.close();
    LOCAL_STACK.close();
  }

  @Override
  public void beforeAll(final ExtensionContext context) throws Exception {
    LOCAL_STACK.start();
    s3Client = S3AsyncClient.builder()
        .endpointOverride(LOCAL_STACK.getEndpoint())
        .credentialsProvider(StaticCredentialsProvider
            .create(AwsBasicCredentials.create(LOCAL_STACK.getAccessKey(), LOCAL_STACK.getSecretKey())))
        .region(Region.of(LOCAL_STACK.getRegion()))
        .build();
  }

  public String getBucketName() {
    return bucketName;
  }
}
