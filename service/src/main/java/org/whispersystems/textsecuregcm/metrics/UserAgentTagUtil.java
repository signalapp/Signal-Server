package org.whispersystems.textsecuregcm.metrics;

import io.micrometer.core.instrument.Tag;
import org.whispersystems.textsecuregcm.util.Pair;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility class for extracting platform/version metrics tags from User-Agent strings.
 */
public class UserAgentTagUtil {

    static final         int                       MAX_VERSIONS       = 10_000;

    public static final  String                    PLATFORM_TAG       = "platform";
    public static final  String                    VERSION_TAG        = "clientVersion";

    static final         List<Tag>                 OVERFLOW_TAGS      = List.of(Tag.of(PLATFORM_TAG, "overflow"), Tag.of(VERSION_TAG, "overflow"));
    static final         List<Tag>                 UNRECOGNIZED_TAGS  = List.of(Tag.of(PLATFORM_TAG, "unrecognized"), Tag.of(VERSION_TAG, "unrecognized"));

    private static final Pattern                   USER_AGENT_PATTERN = Pattern.compile("Signal-([^ ]+) ([^ ]+).*$", Pattern.CASE_INSENSITIVE);

    private static final Set<Pair<String, String>> SEEN_VERSIONS      = new HashSet<>();

    private UserAgentTagUtil() {
    }

    public static List<Tag> getUserAgentTags(final String userAgent) {
        final Matcher matcher = USER_AGENT_PATTERN.matcher(userAgent);
        final List<Tag> tags;

        if (matcher.matches()) {
            final Pair<String, String> platformAndVersion = new Pair<>(matcher.group(1).toLowerCase(), matcher.group(2));

            final boolean allowVersion;

            synchronized (SEEN_VERSIONS) {
                allowVersion = SEEN_VERSIONS.contains(platformAndVersion) || (SEEN_VERSIONS.size() < MAX_VERSIONS && SEEN_VERSIONS.add(platformAndVersion));
            }

            tags = allowVersion ? List.of(Tag.of(PLATFORM_TAG, platformAndVersion.first()), Tag.of(VERSION_TAG, platformAndVersion.second())) : OVERFLOW_TAGS;
        } else {
            tags = UNRECOGNIZED_TAGS;
        }

        return tags;
    }
}
