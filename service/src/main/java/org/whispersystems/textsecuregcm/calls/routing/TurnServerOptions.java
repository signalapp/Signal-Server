/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import java.util.List;

public record TurnServerOptions(String hostname, List<String> urlsWithIps, List<String> urlsWithHostname) {
}
