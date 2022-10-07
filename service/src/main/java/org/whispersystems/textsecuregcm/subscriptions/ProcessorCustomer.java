/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import java.nio.charset.StandardCharsets;
import org.whispersystems.dispatch.util.Util;

public record ProcessorCustomer(String customerId, SubscriptionProcessor processor) {

  public byte[] toDynamoBytes() {
    return Util.combine(new byte[]{processor.getId()}, customerId.getBytes(StandardCharsets.UTF_8));
  }
}
