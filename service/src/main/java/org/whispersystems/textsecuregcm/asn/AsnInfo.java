/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.asn;

import static java.util.Objects.requireNonNull;

import javax.annotation.Nonnull;

public record AsnInfo(long asn, @Nonnull String regionCode) {

  public AsnInfo {
    requireNonNull(regionCode, "regionCode must not be null");
  }
}
