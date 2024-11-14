/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import java.time.Duration;

/**
 * Configuration for Apple DeviceCheck
 *
 * @param production               Whether this is for production or sandbox attestations
 * @param teamId                   The teamId to validate attestations against
 * @param bundleId                 The bundleId to validation attestations against
 */
public record AppleDeviceCheckConfiguration(
    boolean production,
    String teamId,
    String bundleId) {}
