/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import org.whispersystems.textsecuregcm.storage.FeatureFlagsManager;

import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DeleteFeatureFlagTask extends AbstractFeatureFlagTask {

    public DeleteFeatureFlagTask(final FeatureFlagsManager featureFlagsManager) {
        super("delete-feature-flag", featureFlagsManager);
    }

    @Override
    public void execute(final Map<String, List<String>> parameters, final PrintWriter out) {
        if (parameters.containsKey("flag")) {
            for (final String flag : parameters.getOrDefault("flag", Collections.emptyList())) {
                out.println("Deleting feature flag: " + flag);
                getFeatureFlagsManager().deleteFeatureFlag(flag);
            }

            out.println();
            printFeatureFlags(out);
        } else {
            out.println("Usage: delete-feature-flag?flag=FLAG_NAME[&flag=FLAG_NAME2&flag=...]");
        }
    }
}
