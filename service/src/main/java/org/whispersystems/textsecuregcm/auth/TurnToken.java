/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public record TurnToken(
    String username,
    String password,
    @JsonProperty("ttl") long ttlSeconds,
    @Nonnull List<String> urls,
    @Nonnull List<String> urlsWithIps,
    @Nullable String hostname) {
}
