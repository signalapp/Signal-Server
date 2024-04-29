/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.s3;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;

class S3ObjectMonitorTest {

  @Test
  void refresh() {
    final S3Client s3Client = mock(S3Client.class);

    final String bucket = "s3bucket";
    final String objectKey = "greatest-smooth-jazz-hits-of-all-time.zip";

    //noinspection unchecked
    final Consumer<InputStream> listener = mock(Consumer.class);

    final S3ObjectMonitor objectMonitor = new S3ObjectMonitor(
        s3Client,
        bucket,
        objectKey,
        16 * 1024 * 1024,
        mock(ScheduledExecutorService.class),
        Duration.ofMinutes(1));

    final String uuid = UUID.randomUUID().toString();
    when(s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(objectKey).build())).thenReturn(
        HeadObjectResponse.builder().eTag(uuid).build());
    final ResponseInputStream<GetObjectResponse> ris = responseInputStreamFromString("abc", uuid);
    when(s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(objectKey).build())).thenReturn(ris);

    objectMonitor.refresh(listener);
    objectMonitor.refresh(listener);

    verify(listener).accept(ris);
  }

  @Test
  void refreshAfterGet() throws IOException {
    final S3Client s3Client = mock(S3Client.class);

    final String bucket = "s3bucket";
    final String objectKey = "greatest-smooth-jazz-hits-of-all-time.zip";

    //noinspection unchecked
    final Consumer<InputStream> listener = mock(Consumer.class);

    final S3ObjectMonitor objectMonitor = new S3ObjectMonitor(
        s3Client,
        bucket,
        objectKey,
        16 * 1024 * 1024,
        mock(ScheduledExecutorService.class),
        Duration.ofMinutes(1));

    final String uuid = UUID.randomUUID().toString();
    when(s3Client.headObject(HeadObjectRequest.builder().key(objectKey).bucket(bucket).build()))
        .thenReturn(HeadObjectResponse.builder().eTag(uuid).build());
    final ResponseInputStream<GetObjectResponse> responseInputStream = responseInputStreamFromString("abc", uuid);
    when(s3Client.getObject(GetObjectRequest.builder().key(objectKey).bucket(bucket).build())).thenReturn(responseInputStream);

    objectMonitor.getObject();
    objectMonitor.refresh(listener);

    verify(listener, never()).accept(responseInputStream);
  }

  private ResponseInputStream<GetObjectResponse> responseInputStreamFromString(final String s, final String etag) {
    final byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
    final AbortableInputStream ais = AbortableInputStream.create(new ByteArrayInputStream(bytes));
    return new ResponseInputStream<>(GetObjectResponse.builder().contentLength((long) bytes.length).eTag(etag).build(), ais);
  }

  @Test
  void refreshOversizedObject() {
    final S3Client s3Client = mock(S3Client.class);

    final String bucket = "s3bucket";
    final String objectKey = "greatest-smooth-jazz-hits-of-all-time.zip";
    final long maxObjectSize = 16 * 1024 * 1024;

    //noinspection unchecked
    final Consumer<InputStream> listener = mock(Consumer.class);

    final S3ObjectMonitor objectMonitor = new S3ObjectMonitor(
        s3Client,
        bucket,
        objectKey,
        maxObjectSize,
        mock(ScheduledExecutorService.class),
        Duration.ofMinutes(1));

    final String uuid = UUID.randomUUID().toString();
    when(s3Client.headObject(HeadObjectRequest.builder().bucket(bucket).key(objectKey).build())).thenReturn(
        HeadObjectResponse.builder().eTag(uuid).contentLength(maxObjectSize+1).build());
    final ResponseInputStream<GetObjectResponse> ris = responseInputStreamFromString("a".repeat((int) maxObjectSize+1), uuid);
    when(s3Client.getObject(GetObjectRequest.builder().bucket(bucket).key(objectKey).build())).thenReturn(ris);

    objectMonitor.refresh(listener);

    verify(listener, never()).accept(any());
  }
}
