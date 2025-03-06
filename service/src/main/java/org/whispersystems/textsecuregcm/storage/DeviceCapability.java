/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.Optional;

public enum DeviceCapability {
  STORAGE("storage", AccountCapabilityMode.ANY_DEVICE, false, false),
  TRANSFER("transfer", AccountCapabilityMode.PRIMARY_DEVICE, false, false),
  DELETE_SYNC("deleteSync", AccountCapabilityMode.ALL_DEVICES, true, true),
  STORAGE_SERVICE_RECORD_KEY_ROTATION("ssre2", AccountCapabilityMode.ALL_DEVICES, true, true);

  public enum AccountCapabilityMode {
    PRIMARY_DEVICE,
    ANY_DEVICE,
    ALL_DEVICES,
  }

  private final String name;
  private final AccountCapabilityMode accountCapabilityMode;
  private final boolean preventDowngrade;
  private final boolean includeInProfile;

  DeviceCapability(final String name,
      final AccountCapabilityMode accountCapabilityMode,
      final boolean preventDowngrade,
      final boolean includeInProfile) {

    this.name = name;
    this.accountCapabilityMode = accountCapabilityMode;
    this.preventDowngrade = preventDowngrade;
    this.includeInProfile = includeInProfile;
  }

  public String getName() {
    return name;
  }

  public AccountCapabilityMode getAccountCapabilityMode() {
    return accountCapabilityMode;
  }

  public boolean preventDowngrade() {
    return preventDowngrade;
  }

  public boolean includeInProfile() {
    return includeInProfile;
  }

  public static Optional<DeviceCapability> forName(final String name) {
    for (final DeviceCapability capability : DeviceCapability.values()) {
      if (capability.getName().equals(name)) {
        return Optional.of(capability);
      }
    }
    return Optional.empty();
  }
}
