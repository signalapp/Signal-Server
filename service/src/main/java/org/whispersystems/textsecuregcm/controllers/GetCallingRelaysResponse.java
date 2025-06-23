/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import java.util.List;
import org.whispersystems.textsecuregcm.auth.TurnToken;

public record GetCallingRelaysResponse(List<TurnToken> relays) {
}
