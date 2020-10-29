/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.opentable.db.postgres.embedded.LiquibasePreparer;
import com.opentable.db.postgres.junit.EmbeddedPostgresRules;
import com.opentable.db.postgres.junit.PreparedDbRule;
import org.jdbi.v3.core.Jdbi;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;

import java.util.concurrent.ScheduledExecutorService;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class FeatureFlagsManagerTest {

    private FeatureFlagsManager featureFlagsManager;

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(LiquibasePreparer.forClasspathLocation("accountsdb.xml"));

    @Before
    public void setUp() {
        final FaultTolerantDatabase database = new FaultTolerantDatabase("featureFlagsTest",
                Jdbi.create(db.getTestDatabase()),
                new CircuitBreakerConfiguration());

        featureFlagsManager  = new FeatureFlagsManager(new FeatureFlags(database), mock(ScheduledExecutorService.class));
    }

    @Test
    public void testIsFeatureFlagActive() {
        final String flagName = "testFlag";

        assertFalse(featureFlagsManager.isFeatureFlagActive(flagName));

        featureFlagsManager.setFeatureFlag(flagName, true);
        assertTrue(featureFlagsManager.isFeatureFlagActive(flagName));

        featureFlagsManager.setFeatureFlag(flagName, false);
        assertFalse(featureFlagsManager.isFeatureFlagActive(flagName));
    }
}
