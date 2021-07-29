/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.experiment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicExperimentEnrollmentConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicPreRegistrationExperimentEnrollmentConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

class ExperimentEnrollmentManagerTest {

  private DynamicExperimentEnrollmentConfiguration experimentEnrollmentConfiguration;
  private DynamicPreRegistrationExperimentEnrollmentConfiguration preRegistrationExperimentEnrollmentConfiguration;

  private ExperimentEnrollmentManager experimentEnrollmentManager;

  private Account account;

  private static final UUID ACCOUNT_UUID = UUID.randomUUID();
  private static final String UUID_EXPERIMENT_NAME = "uuid_test";

  private static final String ENROLLED_164 = "+12025551212";
  private static final String EXCLUDED_164 = "+18005551212";
  private static final String E164_EXPERIMENT_NAME = "e164_test";

  @BeforeEach
  void setUp() {
    final DynamicConfigurationManager dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);

    experimentEnrollmentManager = new ExperimentEnrollmentManager(dynamicConfigurationManager);

    experimentEnrollmentConfiguration = mock(DynamicExperimentEnrollmentConfiguration.class);
    preRegistrationExperimentEnrollmentConfiguration = mock(
        DynamicPreRegistrationExperimentEnrollmentConfiguration.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getExperimentEnrollmentConfiguration(UUID_EXPERIMENT_NAME))
        .thenReturn(Optional.of(experimentEnrollmentConfiguration));
    when(dynamicConfiguration.getPreRegistrationEnrollmentConfiguration(E164_EXPERIMENT_NAME))
        .thenReturn(Optional.of(preRegistrationExperimentEnrollmentConfiguration));

    account = mock(Account.class);
    when(account.getUuid()).thenReturn(ACCOUNT_UUID);
  }

  @Test
  void testIsEnrolled_UuidExperiment() {
    assertFalse(experimentEnrollmentManager.isEnrolled(account.getUuid(), UUID_EXPERIMENT_NAME));
    assertFalse(
        experimentEnrollmentManager.isEnrolled(account.getUuid(), UUID_EXPERIMENT_NAME + "-unrelated-experiment"));

    when(experimentEnrollmentConfiguration.getEnrolledUuids()).thenReturn(Set.of(ACCOUNT_UUID));
    assertTrue(experimentEnrollmentManager.isEnrolled(account.getUuid(), UUID_EXPERIMENT_NAME));

    when(experimentEnrollmentConfiguration.getEnrolledUuids()).thenReturn(Collections.emptySet());
    when(experimentEnrollmentConfiguration.getEnrollmentPercentage()).thenReturn(0);

    assertFalse(experimentEnrollmentManager.isEnrolled(account.getUuid(), UUID_EXPERIMENT_NAME));

    when(experimentEnrollmentConfiguration.getEnrollmentPercentage()).thenReturn(100);
    assertTrue(experimentEnrollmentManager.isEnrolled(account.getUuid(), UUID_EXPERIMENT_NAME));
  }

  @ParameterizedTest
  @MethodSource
  void testIsEnrolled_PreRegistrationExperiment(final String e164, final String experimentName,
      final Set<String> enrolledE164s, final Set<String> excludedE164s, final Set<String> includedCountryCodes,
      final Set<String> excludedCountryCodes,
      final int enrollmentPercentage,
      final boolean expectEnrolled, final String message) {

    when(preRegistrationExperimentEnrollmentConfiguration.getEnrolledE164s()).thenReturn(enrolledE164s);
    when(preRegistrationExperimentEnrollmentConfiguration.getExcludedE164s()).thenReturn(excludedE164s);
    when(preRegistrationExperimentEnrollmentConfiguration.getEnrollmentPercentage()).thenReturn(enrollmentPercentage);
    when(preRegistrationExperimentEnrollmentConfiguration.getIncludedCountryCodes()).thenReturn(includedCountryCodes);
    when(preRegistrationExperimentEnrollmentConfiguration.getExcludedCountryCodes()).thenReturn(excludedCountryCodes);

    assertEquals(expectEnrolled, experimentEnrollmentManager.isEnrolled(e164, experimentName), message);
  }

  @SuppressWarnings("unused")
  static Stream<Arguments> testIsEnrolled_PreRegistrationExperiment() {
    return Stream.of(
        Arguments.of(ENROLLED_164, E164_EXPERIMENT_NAME, Collections.emptySet(), Collections.emptySet(),
            Collections.emptySet(), Collections.emptySet(), 0, false, "default configuration expects no enrollment"),
        Arguments.of(ENROLLED_164, E164_EXPERIMENT_NAME + "-unrelated-experiment", Collections.emptySet(),
            Collections.emptySet(), Collections.emptySet(), Collections.emptySet(), 0, false,
            "unknown experiment expects no enrollment"),
        Arguments.of(ENROLLED_164, E164_EXPERIMENT_NAME, Set.of(ENROLLED_164), Set.of(EXCLUDED_164),
            Collections.emptySet(), Collections.emptySet(), 0, true, "explicitly enrolled E164 overrides 0% rollout"),
        Arguments.of(ENROLLED_164, E164_EXPERIMENT_NAME, Set.of(ENROLLED_164), Set.of(EXCLUDED_164),
            Collections.emptySet(), Set.of("1"), 0, true, "explicitly enrolled E164 overrides excluded country code"),
        Arguments.of(ENROLLED_164, E164_EXPERIMENT_NAME, Collections.emptySet(), Collections.emptySet(), Set.of("1"),
            Collections.emptySet(), 0, true, "included country code overrides 0% rollout"),
        Arguments.of(EXCLUDED_164, E164_EXPERIMENT_NAME, Collections.emptySet(), Set.of(EXCLUDED_164), Set.of("1"),
            Collections.emptySet(), 100, false, "excluded E164 overrides 100% rollout"),
        Arguments.of(ENROLLED_164, E164_EXPERIMENT_NAME, Collections.emptySet(), Collections.emptySet(),
            Collections.emptySet(), Set.of("1"), 100, false, "excluded country code overrides 100% rollout"),
        Arguments.of(ENROLLED_164, E164_EXPERIMENT_NAME, Collections.emptySet(), Collections.emptySet(),
            Collections.emptySet(), Collections.emptySet(), 100, true, "enrollment expected for 100% rollout")
    );
  }
}
