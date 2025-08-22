/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.Optional;

public enum DeviceCapability {
  STORAGE("storage", AccountCapabilityMode.ANY_DEVICE, false, false),
  TRANSFER("transfer", AccountCapabilityMode.PRIMARY_DEVICE, false, false),
  @Deprecated(forRemoval = true) // Can be removed sometime after 11/10/2025
  DELETE_SYNC("deleteSync", AccountCapabilityMode.ALWAYS_CAPABLE, true, true),
  @Deprecated(forRemoval = true) // Can be removed sometime after 11/10/2025
  STORAGE_SERVICE_RECORD_KEY_ROTATION("ssre2", AccountCapabilityMode.ALWAYS_CAPABLE, true, true),
  ATTACHMENT_BACKFILL("attachmentBackfill", AccountCapabilityMode.PRIMARY_DEVICE, false, true),
  SPARSE_POST_QUANTUM_RATCHET("spqr", AccountCapabilityMode.ALL_DEVICES, false, true);

  public enum AccountCapabilityMode {
    /**
     * The account will have the capability iff the primary device has the capability
     */
    PRIMARY_DEVICE,
    /**
     * The account will have the capability iff any device on the account has the capability
     */
    ANY_DEVICE,
    /**
     * The account will have the capability iff all devices on the account have the capability
     */
    ALL_DEVICES,
    /**
     * The account always has this capability, regardless of the constituent devices' capabilities.
     * This supports retiring capabilities where older clients still need the field provided.
     */
    ALWAYS_CAPABLE,
  }

  private final String name;
  private final AccountCapabilityMode accountCapabilityMode;
  private final boolean preventDowngrade;
  private final boolean includeInProfile;

  /**
   * Create a DeviceCapability
   *
   * @param name                  The name of the device capability that clients will see
   * @param accountCapabilityMode How to combine the constituent device's capabilities in the account to an overall
   *                              account capability
   * @param preventDowngrade      If true, don't let linked devices join that don't have a device capability if the
   *                              overall account has the capability. Most of the time this should only be used in
   *                              conjunction with AccountCapabilityMode.ALL_DEVICES
   * @param includeInProfile      Whether to return this capability on the account's profile. If false, the capability
   *                              is only visible to the server
   */
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
