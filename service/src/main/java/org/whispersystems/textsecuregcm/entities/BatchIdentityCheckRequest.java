/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import java.util.List;
import java.util.UUID;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public record BatchIdentityCheckRequest(@Valid @Size(max = 1000) List<Element> elements) {

  /**
   * @param aci         account id
   * @param fingerprint most significant 4 bytes of SHA-256 of the 33-byte identity key field (32-byte curve25519
   *                    public key prefixed with 0x05)
   */
  public record Element(@NotNull UUID aci, @NotNull @ExactlySize(4) byte[] fingerprint) {

  }
}
