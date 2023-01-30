/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;

import java.util.List;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import org.whispersystems.textsecuregcm.util.E164;

public record AuthCheckRequest(@NotNull @E164 String number,
                               @NotEmpty @Size(max = 10) List<String> passwords) {
}
