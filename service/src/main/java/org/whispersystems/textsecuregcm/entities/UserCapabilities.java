/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import org.whispersystems.textsecuregcm.storage.Account;

public record UserCapabilities(boolean paymentActivation,
                               // TODO Remove the PNI and PNP capabilities entirely on or after 2024-05-18
                               boolean pni,
                               boolean pnp,
                               // TODO Remove the giftBadges capability on or after 2024-05-26
                               boolean giftBadges) {

  public static UserCapabilities createForAccount(final Account account) {
    return new UserCapabilities(account.isPaymentActivationSupported(), true, true, true);
  }
}
