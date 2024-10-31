/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.whispersystems.textsecuregcm.s3.S3ObjectMonitor;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

@JsonTypeName("static")
public class StaticS3ObjectMonitorFactory implements S3ObjectMonitorFactory {

  @JsonProperty
  private String object = "";

  @Override
  public S3ObjectMonitor build(final AwsCredentialsProvider awsCredentialsProvider,
      final ScheduledExecutorService refreshExecutorService) {
    return new StaticS3ObjectMonitor(object, awsCredentialsProvider);
  }

  private static class StaticS3ObjectMonitor extends S3ObjectMonitor {

    private final String object;

    public StaticS3ObjectMonitor(final String object, final AwsCredentialsProvider awsCredentialsProvider) {
      super(awsCredentialsProvider, "local-test-region", "test-bucket", null, 0L, null, null);

      this.object = object;
    }

    @Override
    public synchronized void start(final Consumer<InputStream> changeListener) {
      changeListener.accept(new ByteArrayInputStream(object.getBytes()));
    }
  }
}
