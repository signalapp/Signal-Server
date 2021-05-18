package org.whispersystems.textsecuregcm.util;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class S3ObjectMonitorTest {

  @Test
  void refresh() {
    final AmazonS3 s3Client = mock(AmazonS3.class);
    final ObjectMetadata metadata = mock(ObjectMetadata.class);
    final S3Object s3Object = mock(S3Object.class);

    final String bucket = "s3bucket";
    final String objectKey = "greatest-smooth-jazz-hits-of-all-time.zip";

    //noinspection unchecked
    final Consumer<S3Object> listener = mock(Consumer.class);

    final S3ObjectMonitor objectMonitor = new S3ObjectMonitor(
        s3Client,
        bucket,
        objectKey,
        16 * 1024 * 1024,
        mock(ScheduledExecutorService.class),
        Duration.ofMinutes(1),
        listener);

    when(metadata.getETag()).thenReturn(UUID.randomUUID().toString());
    when(s3Object.getObjectMetadata()).thenReturn(metadata);
    when(s3Client.getObjectMetadata(bucket, objectKey)).thenReturn(metadata);
    when(s3Client.getObject(bucket, objectKey)).thenReturn(s3Object);

    objectMonitor.refresh();
    objectMonitor.refresh();

    verify(listener).accept(s3Object);
  }

  @Test
  void refreshOversizedObject() {
    final AmazonS3 s3Client = mock(AmazonS3.class);
    final ObjectMetadata metadata = mock(ObjectMetadata.class);
    final S3Object s3Object = mock(S3Object.class);

    final String bucket = "s3bucket";
    final String objectKey = "greatest-smooth-jazz-hits-of-all-time.zip";
    final long maxObjectSize = 16 * 1024 * 1024;

    //noinspection unchecked
    final Consumer<S3Object> listener = mock(Consumer.class);

    final S3ObjectMonitor objectMonitor = new S3ObjectMonitor(
        s3Client,
        bucket,
        objectKey,
        maxObjectSize,
        mock(ScheduledExecutorService.class),
        Duration.ofMinutes(1),
        listener);

    when(metadata.getETag()).thenReturn(UUID.randomUUID().toString());
    when(metadata.getContentLength()).thenReturn(maxObjectSize + 1);
    when(s3Object.getObjectMetadata()).thenReturn(metadata);
    when(s3Client.getObjectMetadata(bucket, objectKey)).thenReturn(metadata);
    when(s3Client.getObject(bucket, objectKey)).thenReturn(s3Object);

    objectMonitor.refresh();

    verify(listener, never()).accept(any());
  }
}
