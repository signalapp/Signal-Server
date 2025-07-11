/*
 * Copyright 2013 Signal Messenger, LLC
 * Copyright 2025 Molly Instant Messenger
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.push;

import io.dropwizard.lifecycle.Managed;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

public class DummySender implements Managed, PushNotificationSender {
  private final String originalProvider;

  public DummySender(String originalProvider) {
    this.originalProvider = originalProvider;
  }

  @Override
  public CompletableFuture<SendPushNotificationResult> sendNotification(final PushNotification notification) {
    System.out.printf(
        "Dummy notification sent: provider=%s, type=%s, urgent=%b, "
            + "destination=%s, destinationDevice=%s, data=%s%n",
        this.originalProvider,
        notification.notificationType(),
        notification.urgent(),
        Objects.toString(notification.destination(), ""),
        Objects.toString(notification.destinationDevice(), ""),
        notification.data()
    );

    SendPushNotificationResult result =
        new SendPushNotificationResult(true, null, false, null);

    return CompletableFuture.completedFuture(result);
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }
}
