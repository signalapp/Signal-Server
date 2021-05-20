/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

/**
 * An S3 object monitor watches a specific object in an S3 bucket and notifies a listener if that object changes.
 */
public class S3ObjectMonitor implements Managed {

  private final String s3Bucket;
  private final String objectKey;
  private final long maxObjectSize;

  private final ScheduledExecutorService refreshExecutorService;
  private final Duration refreshInterval;
  private ScheduledFuture<?> refreshFuture;

  private final Consumer<ResponseInputStream<GetObjectResponse>> changeListener;

  private final AtomicReference<String> lastETag = new AtomicReference<>();

  private final S3Client s3Client;

  private static final Logger log = LoggerFactory.getLogger(S3ObjectMonitor.class);

  public S3ObjectMonitor(
      final String s3Region,
      final String s3Bucket,
      final String objectKey,
      final long maxObjectSize,
      final ScheduledExecutorService refreshExecutorService,
      final Duration refreshInterval,
      final Consumer<ResponseInputStream<GetObjectResponse>> changeListener) {

    this(S3Client.builder()
            .region(Region.of(s3Region))
            .credentialsProvider(InstanceProfileCredentialsProvider.create())
            .build(),
        s3Bucket,
        objectKey,
        maxObjectSize,
        refreshExecutorService,
        refreshInterval,
        changeListener);
  }

  @VisibleForTesting
  S3ObjectMonitor(
      final S3Client s3Client,
      final String s3Bucket,
      final String objectKey,
      final long maxObjectSize,
      final ScheduledExecutorService refreshExecutorService,
      final Duration refreshInterval,
      final Consumer<ResponseInputStream<GetObjectResponse>> changeListener) {

    this.s3Client = s3Client;
    this.s3Bucket = s3Bucket;
    this.objectKey = objectKey;
    this.maxObjectSize = maxObjectSize;

    this.refreshExecutorService = refreshExecutorService;
    this.refreshInterval = refreshInterval;

    this.changeListener = changeListener;
  }

  @Override
  public synchronized void start() {
    if (refreshFuture != null) {
      throw new RuntimeException("S3 object manager already started");
    }

    // Run the first request immediately/blocking, then start subsequent calls.
    log.info("Initial request for s3://{}/{}", s3Bucket, objectKey);
    refresh();

    refreshFuture = refreshExecutorService
        .scheduleAtFixedRate(this::refresh, refreshInterval.toMillis(), refreshInterval.toMillis(),
            TimeUnit.MILLISECONDS);
  }

  @Override
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
    ResponseInputStream<GetObjectResponse> response = s3Client.getObject(GetObjectRequest.builder()
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
  void refresh() {
    try {
      final HeadObjectResponse objectMetadata = s3Client.headObject(HeadObjectRequest.builder()
          .bucket(s3Bucket)
          .key(objectKey)
          .build());

      final String initialETag = lastETag.get();
      final String refreshedETag = objectMetadata.eTag();

      if (!StringUtils.equals(initialETag, refreshedETag) && lastETag.compareAndSet(initialETag, refreshedETag)) {
        final ResponseInputStream<GetObjectResponse> response = getObject();

        log.info("Object at s3://{}/{} has changed; new eTag is {} and object size is {} bytes",
            s3Bucket, objectKey, response.response().eTag(), response.response().contentLength());

        try {
          changeListener.accept(response);
        } finally {
          response.close();
        }
      }
    } catch (final Exception e) {
      log.warn("Failed to refresh monitored object", e);
    }
  }
}
