/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.experiment;

import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicExperimentEnrollmentConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public class ExperimentEnrollmentManager {

    private final DynamicConfigurationManager dynamicConfigurationManager;

    public ExperimentEnrollmentManager(final DynamicConfigurationManager dynamicConfigurationManager) {
        this.dynamicConfigurationManager = dynamicConfigurationManager;
    }

    public boolean isEnrolled(final Account account, final String experimentName) {
        final Optional<DynamicExperimentEnrollmentConfiguration> maybeConfiguration = dynamicConfigurationManager.getConfiguration().getExperimentEnrollmentConfiguration(experimentName);

        final Set<UUID> enrolledUuids = maybeConfiguration.map(DynamicExperimentEnrollmentConfiguration::getEnrolledUuids)
                                                          .orElse(Collections.emptySet());

        final boolean enrolled;

        if (enrolledUuids.contains(account.getUuid())) {
            enrolled = true;
        } else {
            final int threshold = maybeConfiguration.map(DynamicExperimentEnrollmentConfiguration::getEnrollmentPercentage).orElse(0);
            final int enrollmentHash = ((account.getUuid().hashCode() ^ experimentName.hashCode()) & Integer.MAX_VALUE) % 100;

            enrolled = enrollmentHash < threshold;
        }

        return enrolled;
    }
}
