/*
 * Copyright 2013-2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

public record UserCapabilities(
    // TODO: Remove the paymentActivation capability entirely sometime soon after 2024-06-30
    boolean paymentActivation) {

  public UserCapabilities() {
    this(true);
  }
}
