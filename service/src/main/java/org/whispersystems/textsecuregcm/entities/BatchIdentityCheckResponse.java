/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import org.signal.libsignal.protocol.IdentityKey;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.IdentityKeyAdapter;
import org.whispersystems.textsecuregcm.util.ServiceIdentifierAdapter;

public record BatchIdentityCheckResponse(@Valid List<Element> elements) {

  public record Element(@JsonInclude(JsonInclude.Include.NON_EMPTY)
                        @JsonSerialize(using = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
                        @JsonDeserialize(using = ServiceIdentifierAdapter.ServiceIdentifierDeserializer.class)
                        @NotNull
                        ServiceIdentifier uuid,

                        @NotNull
                        @JsonSerialize(using = IdentityKeyAdapter.Serializer.class)
                        @JsonDeserialize(using = IdentityKeyAdapter.Deserializer.class)
                        IdentityKey identityKey) {
  }
}
