package org.whispersystems.textsecuregcm.metrics;

import io.micrometer.core.instrument.Tag;
import org.whispersystems.textsecuregcm.util.Pair;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for extracting platform/version metrics tags from User-Agent strings.
 */
public class UserAgentTagUtil {

    public static final  String    PLATFORM_TAG      = "platform";
    public static final  String    VERSION_TAG       = "clientVersion";
    static final         List<Tag> OVERFLOW_TAGS     = List.of(Tag.of(PLATFORM_TAG, "overflow"), Tag.of(VERSION_TAG, "overflow"));
    static final         List<Tag> UNRECOGNIZED_TAGS = List.of(Tag.of(PLATFORM_TAG, "unrecognized"), Tag.of(VERSION_TAG, "unrecognized"));

    private static final Map<String, Pattern> PATTERNS_BY_PLATFORM = Map.of(
            "android", Pattern.compile("^Signal-Android (4[^ ]+).*$", Pattern.CASE_INSENSITIVE),
            "desktop", Pattern.compile("^Signal Desktop (1[^ ]+).*$", Pattern.CASE_INSENSITIVE),
            "ios", Pattern.compile("^Signal/(3[^ ]+) \\(.*ios.*\\)$", Pattern.CASE_INSENSITIVE));

    static final         int                       MAX_VERSIONS  = 10_000;
    private static final Set<Pair<String, String>> SEEN_VERSIONS = new HashSet<>();

    private UserAgentTagUtil() {
    }

    public static List<Tag> getUserAgentTags(final String userAgent) {
        if (userAgent != null) {
            for (final Map.Entry<String, Pattern> entry : PATTERNS_BY_PLATFORM.entrySet()) {
                final String  platform = entry.getKey();
                final Pattern pattern  = entry.getValue();
                final Matcher matcher  = pattern.matcher(userAgent);

                if (matcher.matches()) {
                    final String version = matcher.group(1);

                    return allowVersion(platform, version) ? List.of(Tag.of(PLATFORM_TAG, platform), Tag.of(VERSION_TAG, version)) : OVERFLOW_TAGS;
                }
            }
        }

        return UNRECOGNIZED_TAGS;
    }

    private static boolean allowVersion(final String platform, final String version) {
        final Pair<String, String> platformAndVersion = new Pair<>(platform, version);

        synchronized (SEEN_VERSIONS) {
            return SEEN_VERSIONS.contains(platformAndVersion) || (SEEN_VERSIONS.size() < MAX_VERSIONS && SEEN_VERSIONS.add(platformAndVersion));
        }
    }
}
