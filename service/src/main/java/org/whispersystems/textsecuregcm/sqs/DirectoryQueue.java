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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.MessageAttributeValue;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;

public class DirectoryQueue {

  private static final Logger  logger = LoggerFactory.getLogger(DirectoryQueue.class);

  private final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter serviceErrorMeter = metricRegistry.meter(name(DirectoryQueue.class, "serviceError"));
  private final Meter clientErrorMeter = metricRegistry.meter(name(DirectoryQueue.class, "clientError"));
  private final Timer sendMessageBatchTimer = metricRegistry.timer(name(DirectoryQueue.class, "sendMessageBatch"));

  private final List<String>   queueUrls;
  private final SqsClient      sqs;

  public DirectoryQueue(SqsConfiguration sqsConfig) {
    StaticCredentialsProvider credentialsProvider = StaticCredentialsProvider.create(AwsBasicCredentials.create(
        sqsConfig.getAccessKey(), sqsConfig.getAccessSecret()));

    this.queueUrls = sqsConfig.getQueueUrls();
    this.sqs = SqsClient.builder()
        .region(Region.of(sqsConfig.getRegion()))
        .credentialsProvider(credentialsProvider)
        .build();
  }

  @VisibleForTesting
  DirectoryQueue(final List<String> queueUrls, final SqsClient sqs) {
    this.queueUrls = queueUrls;
    this.sqs = sqs;
  }

  public boolean isDiscoverable(final Account account) {
    return account.isEnabled() && account.isDiscoverableByPhoneNumber();
  }

  public void refreshAccount(final Account account) {
    refreshAccounts(List.of(account));
  }

  public void refreshAccounts(final List<Account> accounts) {
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

          return SendMessageBatchRequestEntry.builder()
              .messageBody("-")
              .id(UUID.randomUUID().toString())
              .messageDeduplicationId(UUID.randomUUID().toString())
              .messageGroupId(account.getNumber())
              .messageAttributes(Map.of(
                  "id", MessageAttributeValue.builder().dataType("String").stringValue(account.getNumber()).build(),
                  "uuid", MessageAttributeValue.builder().dataType("String").stringValue(account.getUuid().toString()).build(),
                  "action", MessageAttributeValue.builder().dataType("String").stringValue(action).build()
              ))
              .build();
        }).collect(Collectors.toList());

        final SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
            .queueUrl(queueUrl)
            .entries(entries)
            .build();

        try (final Timer.Context ignored = sendMessageBatchTimer.time()) {
          sqs.sendMessageBatch(sendMessageBatchRequest);
        } catch (SdkServiceException ex) {
          serviceErrorMeter.mark();
          logger.warn("sqs service error: ", ex);
        } catch (SdkClientException ex) {
          clientErrorMeter.mark();
          logger.warn("sqs client error: ", ex);
        } catch (Throwable t) {
          logger.warn("sqs unexpected error: ", t);
        }
      }
    }
  }
}
