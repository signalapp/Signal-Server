/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public record BatchIdentityCheckResponse(@Valid List<Element> elements) {

  /**
   * Exactly one of {@code aci} and {@code pni} must be non-null
   */
  public record Element(@Nullable UUID aci, @Nullable UUID pni, @NotNull @ExactlySize(33) byte[] identityKey) {

    public Element {
      if (aci == null && pni == null) {
        throw new IllegalArgumentException("aci and pni cannot both be null");
      }

      if (aci != null && pni != null) {
        throw new IllegalArgumentException("aci and pni cannot both be non-null");
      }
    }
  }
}
