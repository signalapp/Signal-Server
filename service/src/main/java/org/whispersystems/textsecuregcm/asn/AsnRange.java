/*
 * Copyright 2013-2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.asn;

import static java.util.Objects.requireNonNull;

import javax.annotation.Nonnull;
import org.apache.commons.lang3.Validate;

public record AsnRange<T extends Comparable<T>>(@Nonnull T from,
                                                @Nonnull T to,
                                                @Nonnull AsnInfo asnInfo) {
  public AsnRange {
    requireNonNull(from);
    requireNonNull(to);
    requireNonNull(asnInfo);
    Validate.isTrue(from.compareTo(to) <= 0);
  }

  boolean contains(@Nonnull final T element) {
    requireNonNull(element);
    return from.compareTo(element) <= 0
        && element.compareTo(to) <= 0;
  }
}
