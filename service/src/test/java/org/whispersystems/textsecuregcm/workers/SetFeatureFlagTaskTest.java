/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.storage.FeatureFlagsManager;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SetFeatureFlagTaskTest {

    private FeatureFlagsManager featureFlagsManager;

    @Before
    public void setUp() {
        featureFlagsManager = mock(FeatureFlagsManager.class);

        when(featureFlagsManager.getAllFlags()).thenReturn(Collections.emptyMap());
    }

    @Test
    public void testExecute() {
        final SetFeatureFlagTask task = new SetFeatureFlagTask(featureFlagsManager);

        task.execute(Map.of("flag", List.of("test-flag"), "active", List.of("true")), mock(PrintWriter.class));

        verify(featureFlagsManager).setFeatureFlag("test-flag", true);
    }
}