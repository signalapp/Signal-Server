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

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class FeatureFlagsTest {

    @Rule
    public PreparedDbRule db = EmbeddedPostgresRules.preparedDatabase(LiquibasePreparer.forClasspathLocation("accountsdb.xml"));

    private FeatureFlags featureFlags;

    @Before
    public void setUp() {
        final FaultTolerantDatabase database = new FaultTolerantDatabase("featureFlagsTest",
                Jdbi.create(db.getTestDatabase()),
                new CircuitBreakerConfiguration());

        this.featureFlags = new FeatureFlags(database);
    }

    @Test
    public void testSetFlagIsFlagActive() {
        assertTrue(featureFlags.getFeatureFlags().isEmpty());

        featureFlags.setFlag("testFlag", true);
        assertEquals(Map.of("testFlag", true), featureFlags.getFeatureFlags());

        featureFlags.setFlag("testFlag", false);
        assertEquals(Map.of("testFlag", false), featureFlags.getFeatureFlags());
    }

    @Test
    public void testDeleteFlag() {
        assertTrue(featureFlags.getFeatureFlags().isEmpty());

        featureFlags.setFlag("testFlag", true);
        assertEquals(Map.of("testFlag", true), featureFlags.getFeatureFlags());

        featureFlags.deleteFlag("testFlag");
        assertTrue(featureFlags.getFeatureFlags().isEmpty());
    }

    @Test
    public void testVacuum() {
        featureFlags.setFlag("testFlag", true);
        assertEquals(Map.of("testFlag", true), featureFlags.getFeatureFlags());

        featureFlags.vacuum();
        assertEquals(Map.of("testFlag", true), featureFlags.getFeatureFlags());
    }
}
