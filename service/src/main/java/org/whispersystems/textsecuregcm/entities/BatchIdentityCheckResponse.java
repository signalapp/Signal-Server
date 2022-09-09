/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public record BatchIdentityCheckResponse(@Valid List<Element> elements) {

  public record Element(@Deprecated @JsonInclude(JsonInclude.Include.NON_EMPTY) @Nullable UUID aci,
                        @JsonInclude(JsonInclude.Include.NON_EMPTY) @Nullable UUID uuid,
                        @NotNull @ExactlySize(33) byte[] identityKey) {

    public Element {
      if (aci == null && uuid == null) {
        throw new IllegalArgumentException("aci and uuid cannot both be null");
      }

      if (aci != null && uuid != null) {
        throw new IllegalArgumentException("aci and uuid cannot both be non-null");
      }
    }
  }
}
