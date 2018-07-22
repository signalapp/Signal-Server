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
import com.amazonaws.services.sqs.model.SendMessageRequest;
import com.amazonaws.services.sqs.model.MessageAttributeValue;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.configuration.ContactDiscoveryConfiguration;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.HashMap;
import java.util.Map;

import static com.codahale.metrics.MetricRegistry.name;

public class ContactDiscoveryQueueSender {

  private static final Logger logger = LoggerFactory.getLogger(ContactDiscoveryQueueSender.class);

  private final MetricRegistry metricRegistry    = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
  private final Meter          serviceErrorMeter = metricRegistry.meter(name(ContactDiscoveryQueueSender.class, "serviceError"));
  private final Meter          clientErrorMeter  = metricRegistry.meter(name(ContactDiscoveryQueueSender.class, "clientError"));

  private final String queueUrl;
  private final AmazonSQS sqs;

  public ContactDiscoveryQueueSender(ContactDiscoveryConfiguration config) {
    final AWSCredentials credentials = new BasicAWSCredentials(config.getAccessKey(), config.getAccessSecret());
    final AWSStaticCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(credentials);
    this.queueUrl = config.getQueueUrl();
    this.sqs = AmazonSQSClientBuilder.standard().withCredentials(credentialsProvider).build();
  }

  private void sendMessage(String action, String user) {
    final Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
    messageAttributes.put("id", new MessageAttributeValue().withDataType("String").withStringValue(user));
    messageAttributes.put("action", new MessageAttributeValue().withDataType("String").withStringValue(action));
    SendMessageRequest send_msg_request = new SendMessageRequest()
            .withQueueUrl(queueUrl)
            .withMessageBody("-")
            .withMessageDeduplicationId(user + action)
            .withMessageGroupId(user)
            .withMessageAttributes(messageAttributes);
    try {
      sqs.sendMessage(send_msg_request);
    } catch (AmazonServiceException ex) {
      serviceErrorMeter.mark();
      logger.warn("ContactDiscoveryQueueSender", "sqs service error", ex);
    } catch (AmazonClientException ex) {
      clientErrorMeter.mark();
      logger.warn("ContactDiscoveryQueueSender", "sqs client error", ex);
    }
  }


  public void addRegisteredUser(String user) {
    sendMessage("add", user);
  }

  public void deleteRegisteredUser(String user) {
    sendMessage("delete", user);
  }
}
