package org.whispersystems.textsecuregcm.metrics;

import io.micrometer.core.instrument.Tag;
import org.whispersystems.textsecuregcm.util.Pair;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for extracting platform/version metrics tags from User-Agent strings.
 */
public class UserAgentTagUtil {

    static final         int                       MAX_VERSIONS           = 10_000;

    public static final  String                    PLATFORM_TAG           = "platform";
    public static final  String                    VERSION_TAG            = "clientVersion";

    static final         List<Tag>                 OVERFLOW_TAGS          = List.of(Tag.of(PLATFORM_TAG, "overflow"), Tag.of(VERSION_TAG, "overflow"));
    static final         List<Tag>                 UNRECOGNIZED_TAGS      = List.of(Tag.of(PLATFORM_TAG, "unrecognized"), Tag.of(VERSION_TAG, "unrecognized"));

    private static final Pattern                   USER_AGENT_PATTERN     = Pattern.compile("^Signal[ \\-]([^ ]+) ([^ ]+).*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern                   IOS_USER_AGENT_PATTERN = Pattern.compile("^Signal/([^ ]+) \\(.*ios.*\\)$", Pattern.CASE_INSENSITIVE);

    private static final Set<Pair<String, String>> SEEN_VERSIONS          = new HashSet<>();

    private UserAgentTagUtil() {
    }

    public static List<Tag> getUserAgentTags(final String userAgent) {
        final List<Tag> tags;

        if (userAgent == null) {
            tags = UNRECOGNIZED_TAGS;
        } else {
            tags = getAndroidOrDesktopUserAgentTags(userAgent)
                    .orElseGet(() -> getIOSUserAgentTags(userAgent)
                            .orElse(UNRECOGNIZED_TAGS));
        }

        return tags;
    }

    private static Optional<List<Tag>> getAndroidOrDesktopUserAgentTags(final String userAgent) {
        final Matcher             matcher = USER_AGENT_PATTERN.matcher(userAgent);
        final Optional<List<Tag>> maybeTags;

        if (matcher.matches()) {
            final String platform = matcher.group(1).toLowerCase();
            final String version  = matcher.group(2);

            maybeTags = Optional.of(allowVersion(platform, version) ? List.of(Tag.of(PLATFORM_TAG, platform), Tag.of(VERSION_TAG, version)) : OVERFLOW_TAGS);
        } else {
            maybeTags = Optional.empty();
        }

        return maybeTags;
    }

    private static Optional<List<Tag>> getIOSUserAgentTags(final String userAgent) {
        final Matcher             matcher = IOS_USER_AGENT_PATTERN.matcher(userAgent);
        final Optional<List<Tag>> maybeTags;

        if (matcher.matches()) {
            final String platform = "ios";
            final String version  = matcher.group(1);

            maybeTags = Optional.of(allowVersion(platform, version) ? List.of(Tag.of(PLATFORM_TAG, platform), Tag.of(VERSION_TAG, version)) : OVERFLOW_TAGS);
        } else {
            maybeTags = Optional.empty();
        }

        return maybeTags;
    }

    private static boolean allowVersion(final String platform, final String version) {
        final Pair<String, String> platformAndVersion = new Pair<>(platform, version);

        synchronized (SEEN_VERSIONS) {
            return SEEN_VERSIONS.contains(platformAndVersion) || (SEEN_VERSIONS.size() < MAX_VERSIONS && SEEN_VERSIONS.add(platformAndVersion));
        }
    }
}
