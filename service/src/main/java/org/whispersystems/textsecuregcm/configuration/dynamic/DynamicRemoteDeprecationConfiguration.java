/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.vdurmont.semver4j.Semver;
import jakarta.validation.constraints.NotNull;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

public record DynamicRemoteDeprecationConfiguration(
    @NotNull Map<ClientPlatform, Semver> minimumVersions,
    @NotNull Map<ClientPlatform, Semver> versionsPendingDeprecation,
    @NotNull Map<ClientPlatform, Set<Semver>> blockedVersions,
    @NotNull Map<ClientPlatform, Set<Semver>> versionsPendingBlock) {

  public static DynamicRemoteDeprecationConfiguration DEFAULT = new DynamicRemoteDeprecationConfiguration(
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap(),
      Collections.emptyMap());

  public DynamicRemoteDeprecationConfiguration {
    if (minimumVersions == null) {
      minimumVersions = DEFAULT.minimumVersions();
    }

    if (versionsPendingDeprecation == null) {
      versionsPendingDeprecation = DEFAULT.versionsPendingDeprecation();
    }

    if (blockedVersions == null) {
      blockedVersions = DEFAULT.blockedVersions();
    }

    if (versionsPendingBlock == null) {
      versionsPendingBlock = DEFAULT.versionsPendingBlock();
    }
  }
}
