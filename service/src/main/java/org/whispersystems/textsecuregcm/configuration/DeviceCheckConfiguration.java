/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.configuration;

import java.time.Duration;

/**
 * Configuration for Device Check operations
 *
 * @param backupRedemptionDuration How long to grant backup access for redemptions via device check
 * @param backupRedemptionLevel    What backup level to grant redemptions via device check
 */
public record DeviceCheckConfiguration(Duration backupRedemptionDuration, long backupRedemptionLevel) {}
