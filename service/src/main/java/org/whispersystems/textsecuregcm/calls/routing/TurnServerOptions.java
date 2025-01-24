/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.calls.routing;

import java.util.List;
import java.util.Optional;

public record TurnServerOptions(
    String hostname,
    Optional<List<String>> urlsWithIps,
    Optional<List<String>> urlsWithHostname
) {
}
