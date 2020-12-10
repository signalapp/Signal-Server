/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import io.micrometer.core.instrument.Tag;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnitParamsRunner.class)
public class UserAgentTagUtilTest {

    @Test
    @Parameters(method = "argumentsForTestGetUserAgentTags")
    public void testGetUserAgentTags(final String userAgent, final List<Tag> expectedTags) {
        assertEquals(new HashSet<>(expectedTags),
                     new HashSet<>(UserAgentTagUtil.getUserAgentTags(userAgent)));
    }

    private static List<Tag> platformVersionTags(String platform, String version) {
        return List.of(Tag.of(UserAgentTagUtil.PLATFORM_TAG, platform), Tag.of(UserAgentTagUtil.VERSION_TAG, version));
    }

    @SuppressWarnings("unused")
    private Object argumentsForTestGetUserAgentTags() {
        return new Object[] {
                new Object[] { "This is obviously not a reasonable User-Agent string.", UserAgentTagUtil.UNRECOGNIZED_TAGS },
                new Object[] { null,                                                    UserAgentTagUtil.UNRECOGNIZED_TAGS },
                new Object[] { "Signal-Android 4.53.7 (Android 8.1)",                   platformVersionTags("android", "4.53.7") },
                new Object[] { "Signal Desktop 1.2.3",                                  platformVersionTags("desktop", "1.2.3") },
                new Object[] { "Signal/3.9.0 (iPhone; iOS 12.2; Scale/3.00)",           platformVersionTags("ios", "3.9.0") },
                new Object[] { "Signal-Android 1.2.3 (Android 8.1)",                    UserAgentTagUtil.UNRECOGNIZED_TAGS },
                new Object[] { "Signal Desktop 3.9.0",                                  platformVersionTags("desktop", "3.9.0") },
                new Object[] { "Signal/4.53.7 (iPhone; iOS 12.2; Scale/3.00)",          platformVersionTags("ios", "4.53.7") },
                new Object[] { "Signal-Android 4.68.3 (Android 9)",                     platformVersionTags("android", "4.68.3") },
                new Object[] { "Signal-Android 1.2.3 (Android 4.3)",                    UserAgentTagUtil.UNRECOGNIZED_TAGS },
                new Object[] { "Signal-Android 4.68.3.0-bobsbootlegclient",             UserAgentTagUtil.UNRECOGNIZED_TAGS },
                new Object[] { "Signal Desktop 1.22.45-foo-0",                          UserAgentTagUtil.UNRECOGNIZED_TAGS },
                new Object[] { "Signal Desktop 1.34.5-beta.1-fakeclientemporium",       UserAgentTagUtil.UNRECOGNIZED_TAGS },
                new Object[] { "Signal Desktop 1.32.0-beta.3",                          UserAgentTagUtil.UNRECOGNIZED_TAGS },
        };
    }

    @Test
    public void testGetUserAgentTagsFlooded() {
        for (int i = 0; i < UserAgentTagUtil.MAX_VERSIONS; i++) {
            UserAgentTagUtil.getUserAgentTags(String.format("Signal-Android 4.0.%d (Android 8.1)", i));
        }

        assertEquals(UserAgentTagUtil.OVERFLOW_TAGS,
                UserAgentTagUtil.getUserAgentTags("Signal-Android 4.1.0 (Android 8.1)"));

        final List<Tag> tags = UserAgentTagUtil.getUserAgentTags("Signal-Android 4.0.0 (Android 8.1)");

        assertEquals(2, tags.size());
        assertTrue(tags.contains(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")));
        assertTrue(tags.contains(Tag.of(UserAgentTagUtil.VERSION_TAG, "4.0.0")));
    }

    @Test
    @Parameters(method = "argumentsForTestGetPlatformTag")
    public void testGetPlatformTag(final String userAgent, final Tag expectedTag) {
        assertEquals(expectedTag, UserAgentTagUtil.getPlatformTag(userAgent));
    }

    private Object argumentsForTestGetPlatformTag() {
        return new Object[] {
                new Object[] { "This is obviously not a reasonable User-Agent string.", Tag.of(UserAgentTagUtil.PLATFORM_TAG, "unrecognized") },
                new Object[] { null,                                                    Tag.of(UserAgentTagUtil.PLATFORM_TAG, "unrecognized") },
                new Object[] { "Signal-Android 4.53.7 (Android 8.1)",                   Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android") },
                new Object[] { "Signal Desktop 1.2.3",                                  Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop") },
                new Object[] { "Signal/3.9.0 (iPhone; iOS 12.2; Scale/3.00)",           Tag.of(UserAgentTagUtil.PLATFORM_TAG, "ios") },
                new Object[] { "Signal-Android 1.2.3 (Android 8.1)",                    Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android") },
                new Object[] { "Signal Desktop 3.9.0",                                  Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop") },
                new Object[] { "Signal/4.53.7 (iPhone; iOS 12.2; Scale/3.00)",          Tag.of(UserAgentTagUtil.PLATFORM_TAG, "ios") },
                new Object[] { "Signal-Android 4.68.3 (Android 9)",                     Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android") },
                new Object[] { "Signal-Android 1.2.3 (Android 4.3)",                    Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android") },
                new Object[] { "Signal-Android 4.68.3.0-bobsbootlegclient",             Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android") },
                new Object[] { "Signal Desktop 1.22.45-foo-0",                          Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop") },
                new Object[] { "Signal Desktop 1.34.5-beta.1-fakeclientemporium",       Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop") },
                new Object[] { "Signal Desktop 1.32.0-beta.3",                          Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop") },
        };
    }
}
