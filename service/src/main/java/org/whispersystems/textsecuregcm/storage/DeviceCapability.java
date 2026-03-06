/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public enum DeviceCapability {
  STORAGE("storage", AccountCapabilityMode.ANY_DEVICE, false, false, false),
  TRANSFER("transfer", AccountCapabilityMode.PRIMARY_DEVICE, false, false, false),
  ATTACHMENT_BACKFILL("attachmentBackfill", AccountCapabilityMode.PRIMARY_DEVICE, false, true, false),
  SPARSE_POST_QUANTUM_RATCHET("spqr", AccountCapabilityMode.ALL_DEVICES, true, true, true);

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

  public static final Set<DeviceCapability> CAPABILITIES_REQUIRED_FOR_NEW_DEVICES =
      Arrays.stream(DeviceCapability.values())
          .filter(DeviceCapability::requireForNewDevices)
          .collect(Collectors.toSet());

  private final String name;
  private final AccountCapabilityMode accountCapabilityMode;
  private final boolean preventDowngrade;
  private final boolean includeInProfile;
  private final boolean requireForNewDevices;

  /**
   * Create a DeviceCapability
   *
   * @param name                  The name of the device capability that clients will see
   * @param accountCapabilityMode How to combine the constituent device's capabilities in the account to an overall
   *                              account capability
   * @param preventDowngrade      If true, don't let linked devices join that don't have a device capability if the
   *                              overall account has the capability. Most of the time this should only be used in
   *                              conjunction with AccountCapabilityMode.ALL_DEVICES.
   * @param includeInProfile      Whether to return this capability on the account's profile. If false, the capability
   *                              is only visible to the server.
   * @param requireForNewDevices  If true, prevent device creation if the new device does not have this capability
   */
  DeviceCapability(final String name,
      final AccountCapabilityMode accountCapabilityMode,
      final boolean preventDowngrade,
      final boolean includeInProfile,
      final boolean requireForNewDevices) {

    this.name = name;
    this.accountCapabilityMode = accountCapabilityMode;
    this.preventDowngrade = preventDowngrade;
    this.includeInProfile = includeInProfile;
    this.requireForNewDevices = requireForNewDevices;
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

  public boolean requireForNewDevices() {
    return requireForNewDevices;
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
