/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import com.codahale.metrics.InstrumentedExecutorService;
import com.codahale.metrics.SharedMetricRegistries;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos.Envelope;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.util.Constants;

public class ReceiptSender {

  private final MessageSender messageSender;
  private final AccountsManager accountManager;
  private final ExecutorService executor;

  private static final Logger logger = LoggerFactory.getLogger(ReceiptSender.class);

  public ReceiptSender(final AccountsManager accountManager, final MessageSender messageSender,
      final ExecutorService executor) {
    this.accountManager = accountManager;
    this.messageSender = messageSender;
    this.executor = new InstrumentedExecutorService(executor,
        SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME),
        MetricsUtil.name(ReceiptSender.class, "executor"));
  }

  public void sendReceipt(UUID sourceUuid, long sourceDeviceId, UUID destinationUuid, long messageId) {
    if (sourceUuid.equals(destinationUuid)) {
      return;
    }

    executor.submit(() -> {
      try {
        accountManager.getByAccountIdentifier(destinationUuid).ifPresentOrElse(
            destinationAccount -> {
              final Envelope.Builder message = Envelope.newBuilder()
                  .setServerTimestamp(System.currentTimeMillis())
                  .setSourceUuid(sourceUuid.toString())
                  .setSourceDevice((int) sourceDeviceId)
                  .setDestinationUuid(destinationUuid.toString())
                  .setTimestamp(messageId)
                  .setType(Envelope.Type.SERVER_DELIVERY_RECEIPT)
                  .setUrgent(false);

              for (final Device destinationDevice : destinationAccount.getDevices()) {
                try {
                  messageSender.sendMessage(destinationAccount, destinationDevice, message.build(), false);
                } catch (final NotPushRegisteredException e) {
                  logger.debug("User no longer push registered for delivery receipt: {}", e.getMessage());
                } catch (final Exception e) {
                  logger.warn("Could not send delivery receipt", e);
                }
              }
            },
            () -> logger.info("No longer registered: {}", destinationUuid)
        );

      } catch (final Exception e) {
        // this exception is most likely a Dynamo timeout or a Redis timeout/circuit breaker
        logger.warn("Could not send delivery receipt", e);
      }
    });
  }
}
