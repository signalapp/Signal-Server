/**
 * Copyright (C) 2018 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.SqsConfiguration;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

public class DirectoryQueue {

  private static final Logger  logger = LoggerFactory.getLogger(DirectoryQueue.class);

  private final MetricRegistry metricRegistry    = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter          serviceErrorMeter = metricRegistry.meter(name(DirectoryQueue.class, "serviceError"));
  private final Meter          clientErrorMeter  = metricRegistry.meter(name(DirectoryQueue.class, "clientError"));

  private final String         queueUrl;
  private final AmazonSQS      sqs;

  public DirectoryQueue(SqsConfiguration sqsConfig) {
    final AWSCredentials               credentials         = new BasicAWSCredentials(sqsConfig.getAccessKey(), sqsConfig.getAccessSecret());
    final AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);

    this.queueUrl = sqsConfig.getQueueUrl();
    this.sqs      = AmazonSQSClientBuilder.standard().withRegion(sqsConfig.getRegion()).withCredentials(credentialsProvider).build();
  }

  public void addRegisteredUser(UUID uuid, String number) {
    sendMessage("add", uuid, number);
  }

  public void deleteRegisteredUser(UUID uuid, String number) {
    sendMessage("delete", uuid, number);
  }

  private void sendMessage(String action, UUID uuid, String number) {
    final Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
    messageAttributes.put("id", new MessageAttributeValue().withDataType("String").withStringValue(number));
    messageAttributes.put("uuid", new MessageAttributeValue().withDataType("String").withStringValue(uuid.toString()));
    messageAttributes.put("action", new MessageAttributeValue().withDataType("String").withStringValue(action));
    SendMessageRequest sendMessageRequest = new SendMessageRequest()
            .withQueueUrl(queueUrl)
            .withMessageBody("-")
            .withMessageDeduplicationId(UUID.randomUUID().toString())
            .withMessageGroupId(number)
            .withMessageAttributes(messageAttributes);
    try {
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
