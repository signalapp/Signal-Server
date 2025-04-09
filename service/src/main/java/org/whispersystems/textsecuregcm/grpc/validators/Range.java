/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.grpc.validators;

public record Range(long min, long max) {
  public Range {
    if (min > max) {
      throw new IllegalArgumentException("invalid range values: expected min <= max but have [%d, %d],".formatted(min, max));
    }
  }
}
