/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.sqs;

import static com.codahale.metrics.MetricRegistry.name;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.SqsConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.util.Constants;
import org.whispersystems.textsecuregcm.util.Pair;

public class DirectoryQueue {

  private static final Logger  logger = LoggerFactory.getLogger(DirectoryQueue.class);

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter serviceErrorMeter = metricRegistry.meter(name(DirectoryQueue.class, "serviceError"));
  private final Meter clientErrorMeter = metricRegistry.meter(name(DirectoryQueue.class, "clientError"));
  private final Timer sendMessageBatchTimer = metricRegistry.timer(name(DirectoryQueue.class, "sendMessageBatch"));

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
    refreshRegisteredUsers(List.of(account));
  }

  public void refreshRegisteredUsers(final List<Account> accounts) {
    final List<Pair<Account, String>> accountsAndActions = accounts.stream()
        .map(account -> new Pair<>(account, account.isEnabled() && account.isDiscoverableByPhoneNumber() ? "add" : "delete"))
        .collect(Collectors.toList());

    sendUpdateMessages(accountsAndActions);
  }

  public void deleteAccount(final Account account) {
    sendUpdateMessages(List.of(new Pair<>(account, "delete")));
  }

  private void sendUpdateMessages(final List<Pair<Account, String>> accountsAndActions) {
    for (final String queueUrl : queueUrls) {
      for (final List<Pair<Account, String>> partition : Iterables.partition(accountsAndActions, 10)) {
        final List<SendMessageBatchRequestEntry> entries = partition.stream().map(pair -> {
          final Account account = pair.first();
          final String action = pair.second();

          return new SendMessageBatchRequestEntry()
              .withMessageBody("-")
              .withId(UUID.randomUUID().toString())
              .withMessageDeduplicationId(UUID.randomUUID().toString())
              .withMessageGroupId(account.getNumber())
              .withMessageAttributes(Map.of(
                  "id", new MessageAttributeValue().withDataType("String").withStringValue(account.getNumber()),
                  "uuid", new MessageAttributeValue().withDataType("String").withStringValue(account.getUuid().toString()),
                  "action", new MessageAttributeValue().withDataType("String").withStringValue(action)
              ));
        }).collect(Collectors.toList());

        final SendMessageBatchRequest sendMessageBatchRequest = new SendMessageBatchRequest()
            .withQueueUrl(queueUrl)
            .withEntries(entries);

        try (final Timer.Context ignored = sendMessageBatchTimer.time()) {
          sqs.sendMessageBatch(sendMessageBatchRequest);
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

}
