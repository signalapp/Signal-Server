/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.subscriptions;

public class SubscriberIdCreationNotPermittedException extends SubscriptionException {

  public SubscriberIdCreationNotPermittedException() {
    super(null);
  }
}
