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

    @SuppressWarnings("unused")
    private Object argumentsForTestGetUserAgentTags() {
        return new Object[] {
                new Object[] { "This is obviously not a reasonable User-Agent string.", UserAgentTagUtil.UNRECOGNIZED_TAGS },
                new Object[] { null,                                                    UserAgentTagUtil.UNRECOGNIZED_TAGS },
                new Object[] { "Signal-Android 4.53.7 (Android 8.1)",                   List.of(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android"), Tag.of(UserAgentTagUtil.VERSION_TAG, "4.53.7")) },
                new Object[] { "Signal Desktop 1.2.3",                                  List.of(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop"), Tag.of(UserAgentTagUtil.VERSION_TAG, "1.2.3")) },
                new Object[] { "Signal/3.9.0 (iPhone; iOS 12.2; Scale/3.00)",           List.of(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "ios"), Tag.of(UserAgentTagUtil.VERSION_TAG, "3.9.0")) },
                new Object[] { "Signal-Android 1.2.3 (Android 8.1)",                    UserAgentTagUtil.UNRECOGNIZED_TAGS },
                new Object[] { "Signal Desktop 3.9.0",                                  UserAgentTagUtil.UNRECOGNIZED_TAGS },
                new Object[] { "Signal/4.53.7 (iPhone; iOS 12.2; Scale/3.00)",          UserAgentTagUtil.UNRECOGNIZED_TAGS },
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
}
