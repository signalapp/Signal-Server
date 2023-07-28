/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.annotation.Nullable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.signal.libsignal.protocol.IdentityKey;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.IdentityKeyAdapter;
import org.whispersystems.textsecuregcm.util.ServiceIdentifierAdapter;

public record BatchIdentityCheckResponse(@Valid List<Element> elements) {

  public record Element(@JsonInclude(JsonInclude.Include.NON_EMPTY)
                        @JsonSerialize(using = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
                        @JsonDeserialize(using = ServiceIdentifierAdapter.ServiceIdentifierDeserializer.class)
                        @Nullable
                        ServiceIdentifier uuid,

                        @JsonInclude(JsonInclude.Include.NON_EMPTY)
                        @JsonSerialize(using = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
                        @JsonDeserialize(using = ServiceIdentifierAdapter.AciServiceIdentifierDeserializer.class)
                        @Nullable
                        @Deprecated // remove after 2023-11-01
                        ServiceIdentifier aci,

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
