/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.telephony;

/// Indicates that a request for carrier data failed permanently (e.g. it was affirmatively rejected by the provider)
/// and should not be retried without modification.
public class CarrierDataException extends Exception {

  public CarrierDataException(final String message) {
    super(message);
  }
}
