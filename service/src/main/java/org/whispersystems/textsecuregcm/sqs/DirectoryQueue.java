/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.sqs;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.SqsConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

public class DirectoryQueue {

  private static final Logger  logger = LoggerFactory.getLogger(DirectoryQueue.class);

  private final MetricRegistry metricRegistry    = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter          serviceErrorMeter = metricRegistry.meter(name(DirectoryQueue.class, "serviceError"));
  private final Meter          clientErrorMeter  = metricRegistry.meter(name(DirectoryQueue.class, "clientError"));
  private final Timer          sendMessageTimer  = metricRegistry.timer(name(DirectoryQueue.class, "sendMessage"));

  private final List<String>   queueUrls;
  private final AmazonSQS      sqs;

  public DirectoryQueue(SqsConfiguration sqsConfig) {
    final AWSCredentials               credentials         = new BasicAWSCredentials(sqsConfig.getAccessKey(), sqsConfig.getAccessSecret());
    final AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);

    this.queueUrls = sqsConfig.getQueueUrls();
    this.sqs       = AmazonSQSClientBuilder.standard().withRegion(sqsConfig.getRegion()).withCredentials(credentialsProvider).build();
  }

  @VisibleForTesting
  DirectoryQueue(final List<String> queueUrls, final AmazonSQS sqs) {
    this.queueUrls = queueUrls;
    this.sqs       = sqs;
  }

  public void refreshRegisteredUser(final Account account) {
    sendMessage(account.isEnabled() && account.isDiscoverableByPhoneNumber() ? "add" : "delete", account.getUuid(), account.getNumber());
  }

  public void deleteAccount(final Account account) {
    sendMessage("delete", account.getUuid(), account.getNumber());
  }

  private void sendMessage(String action, UUID uuid, String number) {
    final Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
    messageAttributes.put("id", new MessageAttributeValue().withDataType("String").withStringValue(number));
    messageAttributes.put("uuid", new MessageAttributeValue().withDataType("String").withStringValue(uuid.toString()));
    messageAttributes.put("action", new MessageAttributeValue().withDataType("String").withStringValue(action));

    for (final String queueUrl : queueUrls) {
      final SendMessageRequest sendMessageRequest = new SendMessageRequest()
              .withQueueUrl(queueUrl)
              .withMessageBody("-")
              .withMessageDeduplicationId(UUID.randomUUID().toString())
              .withMessageGroupId(number)
              .withMessageAttributes(messageAttributes);
      try (final Timer.Context ignored = sendMessageTimer.time()) {
        sqs.sendMessage(sendMessageRequest);
      } catch (AmazonServiceException ex) {
        serviceErrorMeter.mark();
        logger.warn("sqs service error: ", ex);
      } catch (AmazonClientException ex) {
        clientErrorMeter.mark();
        logger.warn("sqs client error: ", ex);
      } catch (Throwable t) {
        logger.warn("sqs unexpected error: ", t);
      }
    }
  }

}
