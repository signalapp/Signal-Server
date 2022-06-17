/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import java.util.List;
import java.util.UUID;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import org.whispersystems.textsecuregcm.util.ExactlySize;

public record BatchIdentityCheckResponse(@Valid List<Element> elements) {
  public record Element(@NotNull UUID aci, @NotNull @ExactlySize(33) byte[] identityKey) {}
}
