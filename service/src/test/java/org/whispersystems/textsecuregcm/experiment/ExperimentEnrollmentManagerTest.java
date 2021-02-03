/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.experiment;

import org.junit.Before;
import org.junit.Test;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicExperimentEnrollmentConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExperimentEnrollmentManagerTest {

    private DynamicExperimentEnrollmentConfiguration experimentEnrollmentConfiguration;

    private ExperimentEnrollmentManager experimentEnrollmentManager;

    private Account account;

    private static final UUID ACCOUNT_UUID = UUID.randomUUID();
    private static final String EXPERIMENT_NAME = "test";

    @Before
    public void setUp() {
        final DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
        final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);

        experimentEnrollmentConfiguration = mock(DynamicExperimentEnrollmentConfiguration.class);
        experimentEnrollmentManager = new ExperimentEnrollmentManager(dynamicConfigurationManager);
        account = mock(Account.class);

        when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
        when(dynamicConfiguration.getExperimentEnrollmentConfiguration(EXPERIMENT_NAME)).thenReturn(Optional.of(experimentEnrollmentConfiguration));
        when(account.getUuid()).thenReturn(ACCOUNT_UUID);
    }

    @Test
    public void testIsEnrolled() {
        assertFalse(experimentEnrollmentManager.isEnrolled(account.getUuid(), EXPERIMENT_NAME));
        assertFalse(experimentEnrollmentManager.isEnrolled(account.getUuid(), EXPERIMENT_NAME + "-unrelated-experiment"));

        when(experimentEnrollmentConfiguration.getEnrolledUuids()).thenReturn(Set.of(ACCOUNT_UUID));
        assertTrue(experimentEnrollmentManager.isEnrolled(account.getUuid(), EXPERIMENT_NAME));

        when(experimentEnrollmentConfiguration.getEnrolledUuids()).thenReturn(Collections.emptySet());
        when(experimentEnrollmentConfiguration.getEnrollmentPercentage()).thenReturn(0);

        assertFalse(experimentEnrollmentManager.isEnrolled(account.getUuid(), EXPERIMENT_NAME));

        when(experimentEnrollmentConfiguration.getEnrollmentPercentage()).thenReturn(100);
        assertTrue(experimentEnrollmentManager.isEnrolled(account.getUuid(), EXPERIMENT_NAME));
    }
}
