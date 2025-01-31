/*
 * Copyright 2013 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jvm.ExecutorServiceMetrics;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.identity.AciServiceIdentifier;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;

public class ReceiptSender {

  private final MessageSender messageSender;
  private final AccountsManager accountManager;
  private final ExecutorService executor;

  private static final Logger logger = LoggerFactory.getLogger(ReceiptSender.class);

  public ReceiptSender(final AccountsManager accountManager, final MessageSender messageSender,
      final ExecutorService executor) {
    this.accountManager = accountManager;
    this.messageSender = messageSender;
    this.executor = ExecutorServiceMetrics.monitor(
        Metrics.globalRegistry, executor, MetricsUtil.name(ReceiptSender.class, "executor"), MetricsUtil.PREFIX)
    ;
  }

  public void sendReceipt(ServiceIdentifier sourceIdentifier, byte sourceDeviceId, AciServiceIdentifier destinationIdentifier, long messageId) {
    if (sourceIdentifier.equals(destinationIdentifier)) {
      return;
    }

    executor.submit(() -> {
      try {
        accountManager.getByAccountIdentifier(destinationIdentifier.uuid()).ifPresentOrElse(
            destinationAccount -> {
              final Envelope message = Envelope.newBuilder()
                  .setServerTimestamp(System.currentTimeMillis())
                  .setSourceServiceId(sourceIdentifier.toServiceIdentifierString())
                  .setSourceDevice(sourceDeviceId)
                  .setDestinationServiceId(destinationIdentifier.toServiceIdentifierString())
                  .setClientTimestamp(messageId)
                  .setType(Envelope.Type.SERVER_DELIVERY_RECEIPT)
                  .setUrgent(false)
                  .build();

              try {
                messageSender.sendMessages(destinationAccount, destinationAccount.getDevices().stream()
                    .collect(Collectors.toMap(Device::getId, ignored -> message)));
              } catch (final Exception e) {
                logger.warn("Could not send delivery receipt", e);
              }
            },
            () -> logger.info("No longer registered: {}", destinationIdentifier)
        );

      } catch (final Exception e) {
        // this exception is most likely a Dynamo timeout or a Redis timeout/circuit breaker
        logger.warn("Could not send delivery receipt", e);
      }
    });
  }
}
