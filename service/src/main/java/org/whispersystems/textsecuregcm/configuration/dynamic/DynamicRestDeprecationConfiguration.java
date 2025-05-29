/*
 * Copyright 2025 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.vdurmont.semver4j.Semver;
import java.util.Map;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

public record DynamicRestDeprecationConfiguration(Map<ClientPlatform, PlatformConfiguration> platforms) {
  public record PlatformConfiguration(Semver minimumRestFreeVersion, int universalRolloutPercent) {}
}
