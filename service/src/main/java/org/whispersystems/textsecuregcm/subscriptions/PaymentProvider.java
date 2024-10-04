/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * A set of payment providers used for donations
 */
public enum PaymentProvider {
  // because provider IDs are stored, they should not be reused, and great care
  // must be used if a provider is removed from the list
  STRIPE(1),
  BRAINTREE(2),
  GOOGLE_PLAY_BILLING(3),
  APPLE_APP_STORE(4);

  private static final Map<Integer, PaymentProvider> IDS_TO_PROCESSORS = new HashMap<>();

  static {
    Arrays.stream(PaymentProvider.values())
        .forEach(provider -> IDS_TO_PROCESSORS.put((int) provider.id, provider));
  }

  /**
   * @return the provider associated with the given ID, or {@code null} if none exists
   */
  public static PaymentProvider forId(byte id) {
    return IDS_TO_PROCESSORS.get((int) id);
  }

  private final byte id;

  PaymentProvider(int id) {
    if (id > 255) {
      throw new IllegalArgumentException("ID must fit in one byte: " + id);
    }

    this.id = (byte) id;
  }

  public byte getId() {
    return id;
  }
}
