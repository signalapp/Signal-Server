/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.auth;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

public record TurnToken(
    String username,
    String password,
    @Nonnull List<String> urls,
    @Nonnull List<String> urlsWithIps,
    @Nullable String hostname) {
}
