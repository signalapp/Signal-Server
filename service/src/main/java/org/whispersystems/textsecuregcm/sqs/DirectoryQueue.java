/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.sqs;

import static com.codahale.metrics.MetricRegistry.name;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import io.dropwizard.lifecycle.Managed;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.SqsConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.util.Constants;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

public class DirectoryQueue implements Managed {

  private static final Logger  logger = LoggerFactory.getLogger(DirectoryQueue.class);

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter serviceErrorMeter = metricRegistry.meter(name(DirectoryQueue.class, "serviceError"));
  private final Meter clientErrorMeter = metricRegistry.meter(name(DirectoryQueue.class, "clientError"));
  private final Timer sendMessageBatchTimer = metricRegistry.timer(name(DirectoryQueue.class, "sendMessageBatch"));

  private final List<String> queueUrls;
  private final SqsAsyncClient sqs;

  private final Set<SendMessageRequest> outstandingRequests = Collections.newSetFromMap(new IdentityHashMap<>());

  private enum UpdateAction {
    ADD("add"),
    DELETE("delete");

    private final String action;

    UpdateAction(final String action) {
      this.action = action;
    }

    public MessageAttributeValue toMessageAttributeValue() {
      return MessageAttributeValue.builder().dataType("String").stringValue(action).build();
    }
  }

  public DirectoryQueue(SqsConfiguration sqsConfig) {
    StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(
        sqsConfig.getAccessKey(), sqsConfig.getAccessSecret()));

    this.queueUrls = sqsConfig.getQueueUrls();

    this.sqs = SqsAsyncClient.builder()
        .region(Region.of(sqsConfig.getRegion()))
        .credentialsProvider(credentialsProvider)
        .build();
  }

  @VisibleForTesting
  DirectoryQueue(final List<String> queueUrls, final SqsAsyncClient sqs) {
    this.queueUrls = queueUrls;
    this.sqs = sqs;
  }

  @Override
  public void start() throws Exception {
  }

  @Override
  public void stop() throws Exception {
    synchronized (outstandingRequests) {
      while (!outstandingRequests.isEmpty()) {
        outstandingRequests.wait();
      }
    }

    sqs.close();
  }

  public boolean isDiscoverable(final Account account) {
    return account.isEnabled() && account.isDiscoverableByPhoneNumber();
  }

  public void refreshAccount(final Account account) {
    sendUpdateMessage(account, isDiscoverable(account) ? UpdateAction.ADD : UpdateAction.DELETE);
  }

  public void deleteAccount(final Account account) {
    sendUpdateMessage(account, UpdateAction.DELETE);
  }

  private void sendUpdateMessage(final Account account, final UpdateAction action) {
    for (final String queueUrl : queueUrls) {
      final Timer.Context timerContext = sendMessageBatchTimer.time();

      final SendMessageRequest request = SendMessageRequest.builder()
          .queueUrl(queueUrl)
          .messageBody("-")
          .messageDeduplicationId(UUID.randomUUID().toString())
          .messageGroupId(account.getNumber())
          .messageAttributes(Map.of(
              "id", MessageAttributeValue.builder().dataType("String").stringValue(account.getNumber()).build(),
              "uuid", MessageAttributeValue.builder().dataType("String").stringValue(account.getUuid().toString()).build(),
              "action", action.toMessageAttributeValue()
          ))
          .build();

      synchronized (outstandingRequests) {
        outstandingRequests.add(request);
      }

      sqs.sendMessage(request).whenComplete((response, cause) -> {
        try {
          if (cause instanceof SdkServiceException) {
            serviceErrorMeter.mark();
            logger.warn("sqs service error", cause);
          } else if (cause instanceof SdkClientException) {
            clientErrorMeter.mark();
            logger.warn("sqs client error", cause);
          } else if (cause != null) {
            logger.warn("sqs unexpected error", cause);
          }
        } finally {
          synchronized (outstandingRequests) {
            outstandingRequests.remove(request);
            outstandingRequests.notifyAll();
          }

          timerContext.close();
        }
      });
    }
  }
}
