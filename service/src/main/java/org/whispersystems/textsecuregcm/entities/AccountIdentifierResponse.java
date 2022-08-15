/*
 * Copyright 2022 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.entities;
import javax.validation.constraints.NotNull;
import java.util.UUID;

public record AccountIdentifierResponse(@NotNull UUID uuid) {}
