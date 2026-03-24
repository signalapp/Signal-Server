/*
 * Copyright 2026 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.controllers;

import java.util.Optional;
import java.util.UUID;
import org.whispersystems.textsecuregcm.identity.ServiceIdentifier;

/// Helper methods to centrally define rate-limit key formats used in multiple places
public class RateLimitKeys {

  /// Key for rate-limiting a device's pre-keys
  ///
  /// @param sourceAci            the account identifier of the authenticated device fetching the pre-keys
  /// @param sourceDeviceId       the deviceId of the authenticated device fetching pre-keys
  /// @param targetIdentifier     the [ServiceIdentifier] of the target account
  /// @param targetDeviceId       the deviceId of the target device, empty if fetching all device pre-keys
  /// @param targetRegistrationId the registrationId of the target device, empty if fetching all device pre-keys
  /// @return the rate-limit key
  public static String preKeyLimiterKey(
      final UUID sourceAci,
      final byte sourceDeviceId,
      final ServiceIdentifier targetIdentifier,
      final Optional<Byte> targetDeviceId,
      final Optional<Integer> targetRegistrationId) {
    return String.format("%s.%s__%s.%s.%s",
        sourceAci,
        sourceDeviceId,
        targetIdentifier.uuid(),
        targetDeviceId.map(String::valueOf).orElse("*"),
        targetRegistrationId.map(String::valueOf).orElse("*"));
  }
}
