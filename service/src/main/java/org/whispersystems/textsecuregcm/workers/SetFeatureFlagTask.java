/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import org.whispersystems.textsecuregcm.storage.FeatureFlagsManager;

import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class SetFeatureFlagTask extends AbstractFeatureFlagTask {

    public SetFeatureFlagTask(final FeatureFlagsManager featureFlagsManager) {
        super("set-feature-flag", featureFlagsManager);
    }

    @Override
    public void execute(final Map<String, List<String>> parameters, final PrintWriter out) {
        final Optional<String> maybeFlag = Optional.ofNullable(parameters.get("flag"))
                                                   .flatMap(values -> values.stream().findFirst());

        final Optional<Boolean> maybeActive = Optional.ofNullable(parameters.get("active"))
                                                      .flatMap(values -> values.stream().findFirst())
                                                      .map(Boolean::valueOf);

        if (maybeFlag.isPresent() && maybeActive.isPresent()) {
            getFeatureFlagsManager().setFeatureFlag(maybeFlag.get(), maybeActive.get());

            out.format("Set %s to %s\n", maybeFlag.get(), maybeActive.get());
            out.println();
            printFeatureFlags(out);
        } else {
            out.println("Usage: set-feature-flag?flag=FLAG_NAME&value=[true|false]");
        }
    }
}
