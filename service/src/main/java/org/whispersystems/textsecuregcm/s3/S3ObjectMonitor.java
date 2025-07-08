/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.s3;

import com.google.common.annotations.VisibleForTesting;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/**
 * An S3 object monitor watches a specific object in an S3 bucket and notifies a listener if that object changes.
 */
public class S3ObjectMonitor {

  private final String s3Bucket;
  private final String objectKey;
  private final long maxObjectSize;

  private final ScheduledExecutorService refreshExecutorService;
  private final Duration refreshInterval;
  private ScheduledFuture<?> refreshFuture;

  private final AtomicReference<String> lastETag = new AtomicReference<>();

  private final S3Client s3Client;

  private static final Logger log = LoggerFactory.getLogger(S3ObjectMonitor.class);

  public S3ObjectMonitor(
      final AwsCredentialsProvider awsCredentialsProvider,
      final String s3Region,
      final String s3Bucket,
      final String objectKey,
      final long maxObjectSize,
      final ScheduledExecutorService refreshExecutorService,
      final Duration refreshInterval) {

    this(createS3Client(awsCredentialsProvider, s3Region),
        s3Bucket,
        objectKey,
        maxObjectSize,
        refreshExecutorService,
        refreshInterval);
  }

  // FLT(uoemai): Allow overriding the AWS endpoint for self-hosting.
  private static S3Client createS3Client(
      final AwsCredentialsProvider awsCredentialsProvider,
      final String s3Region) {

    S3ClientBuilder builder = S3Client.builder()
        .region(Region.of(s3Region))
        .credentialsProvider(awsCredentialsProvider);

    final String endpoint = System.getenv("AWS_ENDPOINT_OVERRIDE");
    if (endpoint != null && !endpoint.isEmpty()) {
      builder.endpointOverride(URI.create(endpoint));
      // FLT(uoemai): Do not use bucket name as subdomain.
      builder.serviceConfiguration(
          S3Configuration.builder()
              .pathStyleAccessEnabled(true)
              .build());
    }

    return builder.build();
  }

  @VisibleForTesting
  S3ObjectMonitor(
      final S3Client s3Client,
      final String s3Bucket,
      final String objectKey,
      final long maxObjectSize,
      final ScheduledExecutorService refreshExecutorService,
      final Duration refreshInterval) {

    this.s3Client = s3Client;
    this.s3Bucket = s3Bucket;
    this.objectKey = objectKey;
    this.maxObjectSize = maxObjectSize;

    this.refreshExecutorService = refreshExecutorService;
    this.refreshInterval = refreshInterval;
  }

  public synchronized void start(final Consumer<InputStream> changeListener) {
    if (refreshFuture != null) {
      throw new RuntimeException("S3 object manager already started");
    }

    // Run the first request immediately/blocking, then start subsequent calls.
    log.info("Initial request for s3://{}/{}", s3Bucket, objectKey);
    refresh(changeListener);

    refreshFuture = refreshExecutorService
        .scheduleAtFixedRate(() -> refresh(changeListener), refreshInterval.toMillis(), refreshInterval.toMillis(),
            TimeUnit.MILLISECONDS);
  }

  public synchronized void stop() {
    if (refreshFuture != null) {
      refreshFuture.cancel(true);
    }
  }

  /**
   * Immediately returns the monitored S3 object regardless of whether it has changed since it was last retrieved.
   *
   * @return the current version of the monitored S3 object.  Caller should close() this upon completion.
   * @throws IOException if the retrieved S3 object is larger than the configured maximum size
   */
  @VisibleForTesting
  ResponseInputStream<GetObjectResponse> getObject() throws IOException {
    final ResponseInputStream<GetObjectResponse> response = s3Client.getObject(GetObjectRequest.builder()
        .key(objectKey)
        .bucket(s3Bucket)
        .build());

    lastETag.set(response.response().eTag());

    if (response.response().contentLength() <= maxObjectSize) {
      return response;
    } else {
      log.warn("Object at s3://{}/{} has a size of {} bytes, which exceeds the maximum allowed size of {} bytes",
          s3Bucket, objectKey, response.response().contentLength(), maxObjectSize);
      response.abort();
      throw new IOException("S3 object too large");
    }
  }

  /**
   * Polls S3 for object metadata and notifies the listener provided at construction time if and only if the object has
   * changed since the last call to {@link #getObject()} or {@code refresh()}.
   */
  @VisibleForTesting
  void refresh(final Consumer<InputStream> changeListener) {
    try {
      final HeadObjectResponse objectMetadata = s3Client.headObject(HeadObjectRequest.builder()
          .bucket(s3Bucket)
          .key(objectKey)
          .build());

      final String initialETag = lastETag.get();
      final String refreshedETag = objectMetadata.eTag();

      if (!StringUtils.equals(initialETag, refreshedETag) && lastETag.compareAndSet(initialETag, refreshedETag)) {
        try (final ResponseInputStream<GetObjectResponse> response = getObject()) {
          log.info("Object at s3://{}/{} has changed; new eTag is {} and object size is {} bytes",
              s3Bucket, objectKey, response.response().eTag(), response.response().contentLength());
          changeListener.accept(response);
        }
      }
    } catch (final Exception e) {
      log.warn("Failed to refresh monitored object", e);
    }
  }
}
