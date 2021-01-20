/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.servlets.tasks.Task;
import org.whispersystems.textsecuregcm.storage.FeatureFlagsManager;

import java.io.PrintWriter;

public abstract class AbstractFeatureFlagTask extends Task {

    private final FeatureFlagsManager featureFlagsManager;

    protected AbstractFeatureFlagTask(final String name, final FeatureFlagsManager featureFlagsManager) {
        super(name);

        this.featureFlagsManager = featureFlagsManager;
    }

    protected FeatureFlagsManager getFeatureFlagsManager() {
        return featureFlagsManager;
    }

    protected void printFeatureFlags(final PrintWriter out) {
        out.println("Feature flags:");
        featureFlagsManager.getAllFlags().forEach((flag, active) -> out.println(flag + ": " + active));
    }
}
