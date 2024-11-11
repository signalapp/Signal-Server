/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.util.List;
import javax.annotation.Nullable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;
import org.whispersystems.textsecuregcm.util.ExactlySize;
import org.whispersystems.textsecuregcm.util.ServiceIdentifierAdapter;

public record BatchIdentityCheckRequest(@Valid @NotNull @Size(max = 1000) List<Element> elements) {

  /**
   * @param uuid        account id or phone number id
   * @param fingerprint most significant 4 bytes of SHA-256 of the 33-byte identity key field (32-byte curve25519 public
   *                    key prefixed with 0x05)
   */
  public record Element(@NotNull
                        @JsonSerialize(using = ServiceIdentifierAdapter.ServiceIdentifierSerializer.class)
                        @JsonDeserialize(using = ServiceIdentifierAdapter.ServiceIdentifierDeserializer.class)
                        ServiceIdentifier uuid,

                        @NotNull
                        @ExactlySize(4)
                        byte[] fingerprint) {
 }
}
