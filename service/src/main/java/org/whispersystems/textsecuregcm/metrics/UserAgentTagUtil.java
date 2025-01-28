/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import io.micrometer.core.instrument.Tag;
import java.util.List;
import java.util.Optional;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;

/**
 * Utility class for extracting platform/version metrics tags from User-Agent strings.
 */
public class UserAgentTagUtil {

  public static final String PLATFORM_TAG = "platform";
  public static final String VERSION_TAG = "clientVersion";
  public static final String LIBSIGNAL_TAG = "libsignal";

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

  public static List<Tag> getLibsignalAndPlatformTags(final String userAgentString) {
    String platform;
    boolean libsignal;

    try {
      final UserAgent userAgent = UserAgentUtil.parseUserAgentString(userAgentString);
      platform = userAgent.getPlatform().name().toLowerCase();
      libsignal = userAgent.getAdditionalSpecifiers()
          .map(additionalSpecifiers -> additionalSpecifiers.contains("libsignal"))
          .orElse(false);
    } catch (final UnrecognizedUserAgentException e) {
      platform = "unrecognized";
      libsignal = false;
    }

    return List.of(Tag.of(PLATFORM_TAG, platform), Tag.of(LIBSIGNAL_TAG, String.valueOf(libsignal)));
  }

}
