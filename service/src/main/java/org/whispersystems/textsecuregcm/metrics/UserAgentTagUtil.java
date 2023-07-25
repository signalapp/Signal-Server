/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import com.vdurmont.semver4j.Semver;
import io.micrometer.core.instrument.Tag;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

/**
 * Utility class for extracting platform/version metrics tags from User-Agent strings.
 */
public class UserAgentTagUtil {

  public static final String PLATFORM_TAG = "platform";
  public static final String VERSION_TAG = "clientVersion";

  private UserAgentTagUtil() {
  }

  public static Tag getPlatformTag(final String userAgentString) {
    String platform;

    try {
      platform = UserAgentUtil.parseUserAgentString(userAgentString).getPlatform().name().toLowerCase();
    } catch (final UnrecognizedUserAgentException e) {
      platform = "unrecognized";
    }

    return Tag.of(PLATFORM_TAG, platform);
  }

  public static Optional<Tag> getClientVersionTag(final String userAgentString, final ClientReleaseManager clientReleaseManager) {
    try {
      final UserAgent userAgent = UserAgentUtil.parseUserAgentString(userAgentString);

      if (clientReleaseManager.isVersionActive(userAgent.getPlatform(), userAgent.getVersion())) {
        return Optional.of(Tag.of(VERSION_TAG, userAgent.getVersion().toString()));
      }
    } catch (final UnrecognizedUserAgentException ignored) {
    }

    return Optional.empty();
  }
}
