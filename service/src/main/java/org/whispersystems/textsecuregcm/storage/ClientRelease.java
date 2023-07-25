/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.vdurmont.semver4j.Semver;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import java.time.Instant;

public record ClientRelease(ClientPlatform platform, Semver version, Instant release, Instant expiration) {
}
