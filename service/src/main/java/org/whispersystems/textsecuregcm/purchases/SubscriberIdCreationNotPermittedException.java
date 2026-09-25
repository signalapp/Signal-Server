/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.purchases;

public class SubscriberIdCreationNotPermittedException extends PurchaseException {

  public SubscriberIdCreationNotPermittedException() {
    super(null);
  }
}
