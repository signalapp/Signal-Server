package org.whispersystems.textsecuregcm.storage;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class FeatureFlagsManagerTest {

    private FeatureFlags       featureFlagDatabase;
    private FeatureFlagsManager featureFlagsManager;

    @Before
    public void setUp() {
        featureFlagDatabase = mock(FeatureFlags.class);
        featureFlagsManager = new FeatureFlagsManager(featureFlagDatabase, mock(ScheduledExecutorService.class));
    }

    @Test
    public void testIsFeatureFlagActive() {
        final Map<String, Boolean> featureFlags = new HashMap<>();
        featureFlags.put("testFlag", true);

        when(featureFlagDatabase.getFeatureFlags()).thenReturn(featureFlags);

        assertFalse(featureFlagsManager.isFeatureFlagActive("testFlag"));

        featureFlagsManager.refreshFeatureFlags();

        assertTrue(featureFlagsManager.isFeatureFlagActive("testFlag"));

        featureFlags.put("testFlag", false);
        featureFlagsManager.refreshFeatureFlags();

        assertFalse(featureFlagsManager.isFeatureFlagActive("testFlag"));
    }
}
