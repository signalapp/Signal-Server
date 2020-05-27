package org.whispersystems.textsecuregcm.metrics;

import io.micrometer.core.instrument.Tag;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class UserAgentTagUtilTest {

    @Test
    public void testGetUserAgentTags() {
        assertEquals(UserAgentTagUtil.UNRECOGNIZED_TAGS,
                UserAgentTagUtil.getUserAgentTags("This is obviously not a reasonable User-Agent string."));

        assertEquals(UserAgentTagUtil.UNRECOGNIZED_TAGS, UserAgentTagUtil.getUserAgentTags(null));

        {
            final List<Tag> tags = UserAgentTagUtil.getUserAgentTags("Signal-Android 4.53.7 (Android 8.1)");

            assertEquals(2, tags.size());
            assertTrue(tags.contains(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")));
            assertTrue(tags.contains(Tag.of(UserAgentTagUtil.VERSION_TAG, "4.53.7")));
        }

        {
            final List<Tag> tags = UserAgentTagUtil.getUserAgentTags("Signal Desktop 1.2.3");

            assertEquals(2, tags.size());
            assertTrue(tags.contains(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "desktop")));
            assertTrue(tags.contains(Tag.of(UserAgentTagUtil.VERSION_TAG, "1.2.3")));
        }

        {
            final List<Tag> tags = UserAgentTagUtil.getUserAgentTags("Signal/3.9.0 (iPhone; iOS 12.2; Scale/3.00)");

            assertEquals(2, tags.size());
            assertTrue(tags.contains(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "ios")));
            assertTrue(tags.contains(Tag.of(UserAgentTagUtil.VERSION_TAG, "3.9.0")));
        }
    }

    @Test
    public void testGetUserAgentTagsFlooded() {
        for (int i = 0; i < UserAgentTagUtil.MAX_VERSIONS; i++) {
            UserAgentTagUtil.getUserAgentTags(String.format("Signal-Android 1.0.%d (Android 8.1)", i));
        }

        assertEquals(UserAgentTagUtil.OVERFLOW_TAGS,
                UserAgentTagUtil.getUserAgentTags("Signal-Android 2.0.0 (Android 8.1)"));

        final List<Tag> tags = UserAgentTagUtil.getUserAgentTags("Signal-Android 1.0.0 (Android 8.1)");

        assertEquals(2, tags.size());
        assertTrue(tags.contains(Tag.of(UserAgentTagUtil.PLATFORM_TAG, "android")));
        assertTrue(tags.contains(Tag.of(UserAgentTagUtil.VERSION_TAG, "1.0.0")));
    }
}
