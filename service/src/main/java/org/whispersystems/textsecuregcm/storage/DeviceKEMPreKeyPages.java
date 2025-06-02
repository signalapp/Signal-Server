/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * The prekey pages stored for a particular device
 *
 * @param identifier           The account identifier or phone number identifier that the keys belong to
 * @param deviceId             The device identifier
 * @param currentPage          If present, the active stored page prekeys are being distributed from
 * @param pageIdToLastModified The last modified time for all the device's stored pages, keyed by the pageId
 */
public record DeviceKEMPreKeyPages(
    UUID identifier, byte deviceId,
    Optional<UUID> currentPage,
    Map<UUID, Instant> pageIdToLastModified) {}
