/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.ExternalAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.PublisherInterface;
import com.google.pubsub.v1.TopicName;
import jakarta.validation.constraints.NotBlank;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@JsonTypeName("default")
public record DefaultPubSubPublisherFactory(@NotBlank String project,
                                            @NotBlank String topic,
                                            @NotBlank String credentialConfiguration) implements
    PubSubPublisherFactory {

  @Override
  public PublisherInterface build() throws IOException {

    final FlowControlSettings flowControlSettings = FlowControlSettings.newBuilder()
        .setLimitExceededBehavior(FlowController.LimitExceededBehavior.ThrowException)
        .setMaxOutstandingElementCount(100L)
        .setMaxOutstandingRequestBytes(16 * 1024 * 1024L) // 16MB
        .build();

    final BatchingSettings batchingSettings = BatchingSettings.newBuilder()
        .setFlowControlSettings(flowControlSettings)
        .setDelayThreshold(org.threeten.bp.Duration.ofMillis(10))
        // These thresholds are actually the default, setting them explicitly since creating a custom batchingSettings resets them
        .setElementCountThreshold(100L)
        .setRequestByteThreshold(5000L)
        .build();

    try (final ByteArrayInputStream credentialConfigInputStream =
        new ByteArrayInputStream(credentialConfiguration.getBytes(StandardCharsets.UTF_8))) {

      return Publisher.newBuilder(
              TopicName.of(project, topic))
          .setCredentialsProvider(
              FixedCredentialsProvider.create(ExternalAccountCredentials.fromStream(credentialConfigInputStream)))
          .setBatchingSettings(batchingSettings)
          .build();
    }
  }
}
