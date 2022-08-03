/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.push;

import java.util.concurrent.CompletableFuture;

public interface PushNotificationSender {

  CompletableFuture<SendPushNotificationResult> sendNotification(PushNotification notification);
}
