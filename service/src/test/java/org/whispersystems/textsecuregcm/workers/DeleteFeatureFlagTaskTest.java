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

public class DeleteFeatureFlagTaskTest {

    private FeatureFlagsManager featureFlagsManager;

    @Before
    public void setUp() {
        featureFlagsManager = mock(FeatureFlagsManager.class);

        when(featureFlagsManager.getAllFlags()).thenReturn(Collections.emptyMap());
    }

    @Test
    public void testExecute() {
        final DeleteFeatureFlagTask task = new DeleteFeatureFlagTask(featureFlagsManager);

        task.execute(Map.of("flag", List.of("test-flag-1", "test-flag-2")), mock(PrintWriter.class));
        verify(featureFlagsManager).deleteFeatureFlag("test-flag-1");
        verify(featureFlagsManager).deleteFeatureFlag("test-flag-2");
    }
}