/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.controllers;

import org.whispersystems.textsecuregcm.auth.TurnToken;

import java.util.List;

public record GetCallingRelaysResponse(List<TurnToken> relays) {
}
