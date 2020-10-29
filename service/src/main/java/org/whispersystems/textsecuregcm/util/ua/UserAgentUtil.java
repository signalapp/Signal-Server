/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.util.ua;

import com.google.common.annotations.VisibleForTesting;
import com.vdurmont.semver4j.Semver;
import org.apache.commons.lang3.StringUtils;

import java.util.EnumMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class UserAgentUtil {

    private static final Pattern STANDARD_UA_PATTERN = Pattern.compile("^Signal-(Android|Desktop|iOS)/([^ ]+)( (.+))?$", Pattern.CASE_INSENSITIVE);

    private static final Map<ClientPlatform, Pattern> LEGACY_PATTERNS_BY_PLATFORM = new EnumMap<>(ClientPlatform.class);

    static {
        LEGACY_PATTERNS_BY_PLATFORM.put(ClientPlatform.ANDROID, Pattern.compile("^Signal-Android ([^ ]+)( (.+))?$", Pattern.CASE_INSENSITIVE));
        LEGACY_PATTERNS_BY_PLATFORM.put(ClientPlatform.DESKTOP, Pattern.compile("^Signal Desktop (.+)$", Pattern.CASE_INSENSITIVE));
        LEGACY_PATTERNS_BY_PLATFORM.put(ClientPlatform.IOS, Pattern.compile("^Signal/([^ ]+)( (.+))?$", Pattern.CASE_INSENSITIVE));
    }

    public static UserAgent parseUserAgentString(final String userAgentString) throws UnrecognizedUserAgentException {
        if (StringUtils.isBlank(userAgentString)) {
            throw new UnrecognizedUserAgentException("User-Agent string is blank");
        }

        try {
            final UserAgent standardUserAgent = parseStandardUserAgentString(userAgentString);

            if (standardUserAgent != null) {
                return standardUserAgent;
            }

            final UserAgent legacyUserAgent = parseLegacyUserAgentString(userAgentString);

            if (legacyUserAgent != null) {
                return legacyUserAgent;
            }
        } catch (final Exception e) {
            throw new UnrecognizedUserAgentException(e);
        }

        throw new UnrecognizedUserAgentException();
    }

    @VisibleForTesting
    static UserAgent parseStandardUserAgentString(final String userAgentString) {
        final Matcher matcher = STANDARD_UA_PATTERN.matcher(userAgentString);

        if (matcher.matches()) {
            return new UserAgent(ClientPlatform.valueOf(matcher.group(1).toUpperCase()), new Semver(matcher.group(2)), StringUtils.stripToNull(matcher.group(4)));
        }

        return null;
    }

    @VisibleForTesting
    static UserAgent parseLegacyUserAgentString(final String userAgentString) {
        for (final Map.Entry<ClientPlatform, Pattern> entry : LEGACY_PATTERNS_BY_PLATFORM.entrySet()) {
            final ClientPlatform platform = entry.getKey();
            final Pattern pattern = entry.getValue();
            final Matcher matcher = pattern.matcher(userAgentString);

            if (matcher.matches()) {
                final UserAgent userAgent;

                if (matcher.groupCount() > 1) {
                    userAgent = new UserAgent(platform, new Semver(matcher.group(1)), StringUtils.stripToNull(matcher.group(matcher.groupCount())));
                } else {
                    userAgent = new UserAgent(platform, new Semver(matcher.group(1)));
                }

                return userAgent;
            }
        }

        return null;
    }
}
