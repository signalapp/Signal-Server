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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.signal.libsignal.protocol.IdentityKey;
import org.signal.libsignal.protocol.ecc.ECPublicKey;
import org.whispersystems.textsecuregcm.util.IdentityKeyAdapter;

public record BatchIdentityCheckResponse(@Valid List<Element> elements) {

  public record Element(@Deprecated
                        @JsonInclude(JsonInclude.Include.NON_EMPTY)
                        @Nullable UUID aci,

                        @JsonInclude(JsonInclude.Include.NON_EMPTY)
                        @Nullable UUID uuid,

                        @NotNull
                        @JsonSerialize(using = IdentityKeyAdapter.Serializer.class)
                        @JsonDeserialize(using = IdentityKeyAdapter.Deserializer.class)
                        IdentityKey identityKey) {

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
