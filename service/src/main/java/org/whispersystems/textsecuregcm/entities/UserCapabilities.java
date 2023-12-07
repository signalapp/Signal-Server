/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import org.whispersystems.textsecuregcm.storage.Account;

public record UserCapabilities(
    boolean paymentActivation,
    boolean pni) {

  public static UserCapabilities createForAccount(Account account) {
    return new UserCapabilities(
        account.isPaymentActivationSupported(),

        // Although originally intended to indicate that clients support phone number identifiers, the scope of this
        // flag has expanded to cover phone number privacy in general
        account.isPniSupported());
  }
}
