/*
 * Copyright 2024 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public enum DeviceCapability {
  STORAGE("storage", AccountCapabilityMode.ANY_DEVICE, AccountCapabilityVisibility.SERVER, false, false),
  TRANSFER("transfer", AccountCapabilityMode.PRIMARY_DEVICE, AccountCapabilityVisibility.SERVER, false, false),
  ATTACHMENT_BACKFILL("attachmentBackfill", AccountCapabilityMode.PRIMARY_DEVICE, AccountCapabilityVisibility.SELF, false, false),
  SPARSE_POST_QUANTUM_RATCHET("spqr", AccountCapabilityMode.ALL_DEVICES, AccountCapabilityVisibility.PUBLIC, true, true),
  PROFILES_V2("profiles_v2", AccountCapabilityMode.ALL_DEVICES, AccountCapabilityVisibility.SELF, false, false),
  USERNAME_CHANGE_SYNC_MESSAGE("usernameChangeSyncMessage", AccountCapabilityMode.ALL_DEVICES, AccountCapabilityVisibility.SELF, true, false);

  public static final List<DeviceCapability> PUBLIC_VISIBLE_CAPABILITIES = Arrays.stream(DeviceCapability.values())
      .filter(c -> c.accountCapabilityVisibility == AccountCapabilityVisibility.PUBLIC)
      .toList();

  public static final List<DeviceCapability> SELF_VISIBLE_CAPABILITIES = Arrays.stream(DeviceCapability.values())
      .filter(c -> c.accountCapabilityVisibility == AccountCapabilityVisibility.PUBLIC
              || c.accountCapabilityVisibility == AccountCapabilityVisibility.SELF)
      .toList();

  public enum AccountCapabilityMode {
    /// The account will have the capability iff the primary device has the capability
    PRIMARY_DEVICE,

    /// The account will have the capability iff any device on the account has the capability
    ANY_DEVICE,

    /// The account will have the capability iff all devices on the account have the capability
    ALL_DEVICES,

    /// The account always has this capability, regardless of the constituent devices' capabilities.
    /// This supports retiring capabilities where older clients still need the field provided.
    ALWAYS_CAPABLE,
  }

  public enum AccountCapabilityVisibility {
    /// The capability is only visible to the server.
    SERVER,

    /// The capability is visible to all devices on the account.
    SELF,

    /// The capability is publicly visible.
    PUBLIC
  }

  public static final Set<DeviceCapability> CAPABILITIES_REQUIRED_FOR_NEW_DEVICES =
      EnumSet.copyOf(Arrays.stream(DeviceCapability.values())
          .filter(DeviceCapability::requireForNewDevices)
          .collect(Collectors.toSet()));

  private final String name;
  private final AccountCapabilityMode accountCapabilityMode;
  private final AccountCapabilityVisibility accountCapabilityVisibility;
  private final boolean preventDowngrade;
  private final boolean requireForNewDevices;

  /// Create a DeviceCapability
  ///
  /// @param name                        The name of the device capability that clients will see
  /// @param accountCapabilityMode       How to combine the constituent device's capabilities in the account to an
  ///                                    overall account capability
  /// @param accountCapabilityVisibility Who should be able to view this capability
  /// @param preventDowngrade            If true, don't let linked devices join that don't have a device capability if
  ///                                    the overall account has the capability. Most of the time this should only be
  ///                                    used in conjunction with AccountCapabilityMode.ALL_DEVICES.
  /// @param requireForNewDevices        If true, prevent device creation if the new device does not have this
  ///                                    capability
  DeviceCapability(final String name,
      final AccountCapabilityMode accountCapabilityMode,
      final AccountCapabilityVisibility accountCapabilityVisibility,
      final boolean preventDowngrade,
      final boolean requireForNewDevices) {

    this.name = name;
    this.accountCapabilityMode = accountCapabilityMode;
    this.preventDowngrade = preventDowngrade;
    this.requireForNewDevices = requireForNewDevices;
    this.accountCapabilityVisibility = accountCapabilityVisibility;
  }

  public String getName() {
    return name;
  }

  public AccountCapabilityMode getAccountCapabilityMode() {
    return accountCapabilityMode;
  }

  public AccountCapabilityVisibility getAccountCapabilityVisibility() {
    return accountCapabilityVisibility;
  }

  public boolean preventDowngrade() {
    return preventDowngrade;
  }

  public boolean includeInLegacyProfile() {
    // Profiles v1 does not distinguish between self/public capabilities
    return accountCapabilityVisibility != AccountCapabilityVisibility.SERVER;
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
