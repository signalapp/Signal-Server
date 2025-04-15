/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import io.micrometer.core.instrument.Tag;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.whispersystems.textsecuregcm.WhisperServerVersion;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;
import org.whispersystems.textsecuregcm.util.ua.UnrecognizedUserAgentException;
import org.whispersystems.textsecuregcm.util.ua.UserAgent;
import org.whispersystems.textsecuregcm.util.ua.UserAgentUtil;
import javax.annotation.Nullable;

/**
 * Utility class for extracting platform/version metrics tags from User-Agent strings.
 */
public class UserAgentTagUtil {

  public static final String PLATFORM_TAG = "platform";
  public static final String VERSION_TAG = "clientVersion";
  public static final String LIBSIGNAL_TAG = "libsignal";

  public static final String SERVER_UA =
      String.format("Signal-Server/%s (%s)", WhisperServerVersion.getServerVersion(), UUID.randomUUID());

  private UserAgentTagUtil() {
  }

  public static Tag getPlatformTag(final String userAgentString) {

    if (SERVER_UA.equals(userAgentString)) {
      return Tag.of(PLATFORM_TAG, "server");
    }

    UserAgent userAgent = null;

    try {
      userAgent = UserAgentUtil.parseUserAgentString(userAgentString);
    } catch (final UnrecognizedUserAgentException ignored) {
    }

    return getPlatformTag(userAgent);
  }

  public static Tag getPlatformTag(@Nullable final UserAgent userAgent) {
    return Tag.of(PLATFORM_TAG, userAgent != null ? userAgent.platform().name().toLowerCase() : "unrecognized");
  }

  public static Optional<Tag> getClientVersionTag(final String userAgentString, final ClientReleaseManager clientReleaseManager) {
    try {
      final UserAgent userAgent = UserAgentUtil.parseUserAgentString(userAgentString);

      if (clientReleaseManager.isVersionActive(userAgent.platform(), userAgent.version())) {
        return Optional.of(Tag.of(VERSION_TAG, userAgent.version().toString()));
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
      platform = userAgent.platform().name().toLowerCase();
      libsignal = StringUtils.contains(userAgent.additionalSpecifiers(), "libsignal");
    } catch (final UnrecognizedUserAgentException e) {
      platform = "unrecognized";
      libsignal = false;
    }

    return List.of(Tag.of(PLATFORM_TAG, platform), Tag.of(LIBSIGNAL_TAG, String.valueOf(libsignal)));
  }

}
