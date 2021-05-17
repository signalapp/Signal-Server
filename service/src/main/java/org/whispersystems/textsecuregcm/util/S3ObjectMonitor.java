/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util;

import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.lifecycle.Managed;
import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An S3 object monitor watches a specific object in an S3 bucket and notifies a listener if that object changes.
 */
public class S3ObjectMonitor implements Managed {

  private final String s3Bucket;
  private final String objectKey;

  private final ScheduledExecutorService refreshExecutorService;
  private final Duration refreshInterval;
  private ScheduledFuture<?> refreshFuture;

  private final Consumer<S3Object> changeListener;

  private final AtomicReference<String> lastETag = new AtomicReference<>();

  private final AmazonS3 s3Client;

  private static final Logger log = LoggerFactory.getLogger(S3ObjectMonitor.class);

  public S3ObjectMonitor(
      final String s3Region,
      final String s3Bucket,
      final String objectKey,
      final ScheduledExecutorService refreshExecutorService,
      final Duration refreshInterval,
      final Consumer<S3Object> changeListener) {

    this(AmazonS3ClientBuilder.standard()
            .withCredentials(InstanceProfileCredentialsProvider.getInstance())
            .withRegion(s3Region)
            .build(),
        s3Bucket,
        objectKey,
        refreshExecutorService,
        refreshInterval,
        changeListener);
  }

  @VisibleForTesting
  S3ObjectMonitor(
      final AmazonS3 s3Client,
      final String s3Bucket,
      final String objectKey,
      final ScheduledExecutorService refreshExecutorService,
      final Duration refreshInterval,
      final Consumer<S3Object> changeListener) {

    this.s3Client = s3Client;
    this.s3Bucket = s3Bucket;
    this.objectKey = objectKey;

    this.refreshExecutorService = refreshExecutorService;
    this.refreshInterval = refreshInterval;

    this.changeListener = changeListener;
  }

  @Override
  public synchronized void start() {
    if (refreshFuture != null) {
      throw new RuntimeException("S3 object manager already started");
    }

    refreshFuture = refreshExecutorService
        .scheduleAtFixedRate(this::refresh, refreshInterval.toMillis(), refreshInterval.toMillis(), TimeUnit.MILLISECONDS);
  }

  @Override
  public synchronized void stop() {
    if (refreshFuture != null) {
      refreshFuture.cancel(true);
    }
  }

  /**
   * Polls S3 for object metadata and notifies the listener provided at construction time if and only if the object has
   * changed. This method blocks until S3 object metadata has been retrieved and, if the object has changed, the
   * listener returns. Callers may wish to call this method at initialization time if they need data from the monitored
   * S3 object immediately and don't want to wait for the first scheduled update.
   */
  public void refresh() {
    try {
      final ObjectMetadata objectMetadata = s3Client.getObjectMetadata(s3Bucket, objectKey);

      final String initialETag = lastETag.get();
      final String refreshedETag = objectMetadata.getETag();

      if (!StringUtils.equals(initialETag, refreshedETag) && lastETag.compareAndSet(initialETag, refreshedETag)) {
        changeListener.accept(s3Client.getObject(s3Bucket, objectKey));
      }
    } catch (final Exception e) {
      log.warn("Failed to refresh monitored object", e);
    }
  }
}
