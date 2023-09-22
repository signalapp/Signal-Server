/*
 * Copyright 2023 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.tests.util;

import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicExperimentEnrollmentConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExperimentHelper {

  public static DynamicConfigurationManager<DynamicConfiguration> withEnrollment(
      final String experimentName,
      final Set<UUID> enrolledUuids,
      final int enrollmentPercentage) {
    final DynamicConfigurationManager<DynamicConfiguration> dcm = mock(DynamicConfigurationManager.class);
    final DynamicConfiguration dc = mock(DynamicConfiguration.class);
    when(dcm.getConfiguration()).thenReturn(dc);
    final DynamicExperimentEnrollmentConfiguration exp = mock(DynamicExperimentEnrollmentConfiguration.class);
    when(dc.getExperimentEnrollmentConfiguration(experimentName)).thenReturn(Optional.of(exp));
    when(exp.getEnrolledUuids()).thenReturn(enrolledUuids);
    when(exp.getEnrollmentPercentage()).thenReturn(enrollmentPercentage);
    return dcm;
  }

  public static DynamicConfigurationManager<DynamicConfiguration> withEnrollment(final String experimentName, final Set<UUID> enrolledUuids) {
    return withEnrollment(experimentName, enrolledUuids, 0);
  }

  public static DynamicConfigurationManager<DynamicConfiguration> withEnrollment(final String experimentName, final UUID enrolledUuid) {
    return withEnrollment(experimentName, Set.of(enrolledUuid), 0);
  }
}
