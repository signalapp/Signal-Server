/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.experiment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicExperimentEnrollmentConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicE164ExperimentEnrollmentConfiguration;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

class ExperimentEnrollmentManagerTest {

  private DynamicExperimentEnrollmentConfiguration.UuidSelector uuidSelector;
  private DynamicExperimentEnrollmentConfiguration experimentEnrollmentConfiguration;
  private DynamicE164ExperimentEnrollmentConfiguration e164ExperimentEnrollmentConfiguration;

  private ExperimentEnrollmentManager experimentEnrollmentManager;

  private Account account;
  private Random random;

  private static final UUID ACCOUNT_UUID = UUID.randomUUID();
  private static final UUID EXCLUDED_UUID = UUID.randomUUID();
  private static final String UUID_EXPERIMENT_NAME = "uuid_test";
  private static final String E164_AND_UUID_EXPERIMENT_NAME = "e164_uuid_test";

  private static final String NOT_ENROLLED_164 = "+632025551212";
  private static final String ENROLLED_164 = "+12025551212";
  private static final String EXCLUDED_164 = "+18005551212";
  private static final String E164_EXPERIMENT_NAME = "e164_test";

  @BeforeEach
  void setUp() {
    @SuppressWarnings("unchecked")
    final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager = mock(DynamicConfigurationManager.class);
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    random = spy(new Random());
    experimentEnrollmentManager = new ExperimentEnrollmentManager(dynamicConfigurationManager, () -> random);

    uuidSelector = mock(DynamicExperimentEnrollmentConfiguration.UuidSelector.class);
    when(uuidSelector.getUuidEnrollmentPercentage()).thenReturn(100);

    experimentEnrollmentConfiguration = mock(DynamicExperimentEnrollmentConfiguration.class);
    when(experimentEnrollmentConfiguration.getUuidSelector()).thenReturn(uuidSelector);
    e164ExperimentEnrollmentConfiguration = mock(
        DynamicE164ExperimentEnrollmentConfiguration.class);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getExperimentEnrollmentConfiguration(UUID_EXPERIMENT_NAME))
        .thenReturn(Optional.of(experimentEnrollmentConfiguration));
    when(dynamicConfiguration.getE164ExperimentEnrollmentConfiguration(E164_EXPERIMENT_NAME))
        .thenReturn(Optional.of(e164ExperimentEnrollmentConfiguration));
    when(dynamicConfiguration.getExperimentEnrollmentConfiguration(E164_AND_UUID_EXPERIMENT_NAME))
        .thenReturn(Optional.of(experimentEnrollmentConfiguration));
    when(dynamicConfiguration.getE164ExperimentEnrollmentConfiguration(E164_AND_UUID_EXPERIMENT_NAME))
        .thenReturn(Optional.of(e164ExperimentEnrollmentConfiguration));

    account = mock(Account.class);
    when(account.getUuid()).thenReturn(ACCOUNT_UUID);
  }

  @Test
  void testIsEnrolled_UuidExperiment() {
    assertFalse(experimentEnrollmentManager.isEnrolled(account.getUuid(), UUID_EXPERIMENT_NAME));
    assertFalse(
        experimentEnrollmentManager.isEnrolled(account.getUuid(), UUID_EXPERIMENT_NAME + "-unrelated-experiment"));

    when(uuidSelector.getUuids()).thenReturn(Set.of(ACCOUNT_UUID));
    assertTrue(experimentEnrollmentManager.isEnrolled(account.getUuid(), UUID_EXPERIMENT_NAME));

    when(uuidSelector.getUuids()).thenReturn(Collections.emptySet());
    when(experimentEnrollmentConfiguration.getEnrollmentPercentage()).thenReturn(0);

    assertFalse(experimentEnrollmentManager.isEnrolled(account.getUuid(), UUID_EXPERIMENT_NAME));

    when(experimentEnrollmentConfiguration.getEnrollmentPercentage()).thenReturn(100);
    assertTrue(experimentEnrollmentManager.isEnrolled(account.getUuid(), UUID_EXPERIMENT_NAME));

    when(experimentEnrollmentConfiguration.getExcludedUuids()).thenReturn(Set.of(EXCLUDED_UUID));
    when(experimentEnrollmentConfiguration.getEnrollmentPercentage()).thenReturn(100);
    when(uuidSelector.getUuidEnrollmentPercentage()).thenReturn(100);
    when(uuidSelector.getUuids()).thenReturn(Set.of(EXCLUDED_UUID));
    assertFalse(experimentEnrollmentManager.isEnrolled(EXCLUDED_UUID, UUID_EXPERIMENT_NAME));
  }

  @Test
  void testIsEnrolled_UuidExperimentPercentage() {
    when(uuidSelector.getUuids()).thenReturn(Set.of(ACCOUNT_UUID));
    when(uuidSelector.getUuidEnrollmentPercentage()).thenReturn(0);
    assertFalse(experimentEnrollmentManager.isEnrolled(account.getUuid(), UUID_EXPERIMENT_NAME));
    when(uuidSelector.getUuidEnrollmentPercentage()).thenReturn(100);
    assertTrue(experimentEnrollmentManager.isEnrolled(account.getUuid(), UUID_EXPERIMENT_NAME));

    when(uuidSelector.getUuidEnrollmentPercentage()).thenReturn(75);
    final Map<Boolean, Long> counts = IntStream.range(0, 100).mapToObj(i -> {
          when(random.nextInt(100)).thenReturn(i);
          return experimentEnrollmentManager.isEnrolled(account.getUuid(), UUID_EXPERIMENT_NAME);
        })
        .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    assertEquals(25, counts.get(false));
    assertEquals(75, counts.get(true));
  }

  @Test
  void testIsEnrolled_E164AndUuidExperiment() {
    when(e164ExperimentEnrollmentConfiguration.getIncludedCountryCodes()).thenReturn(Set.of("1"));
    when(e164ExperimentEnrollmentConfiguration.getEnrollmentPercentage()).thenReturn(0);
    when(e164ExperimentEnrollmentConfiguration.getEnrolledE164s()).thenReturn(Collections.emptySet());
    when(e164ExperimentEnrollmentConfiguration.getExcludedE164s()).thenReturn(Collections.emptySet());
    when(e164ExperimentEnrollmentConfiguration.getExcludedCountryCodes()).thenReturn(Collections.emptySet());

    // test UUID enrollment is prioritized
    when(uuidSelector.getUuids()).thenReturn(Set.of(ACCOUNT_UUID));
    when(uuidSelector.getUuidEnrollmentPercentage()).thenReturn(100);
    assertTrue(experimentEnrollmentManager.isEnrolled(NOT_ENROLLED_164, account.getUuid(), E164_AND_UUID_EXPERIMENT_NAME));
    when(uuidSelector.getUuidEnrollmentPercentage()).thenReturn(0);
    assertFalse(experimentEnrollmentManager.isEnrolled(NOT_ENROLLED_164, account.getUuid(), E164_AND_UUID_EXPERIMENT_NAME));
    assertFalse(experimentEnrollmentManager.isEnrolled(ENROLLED_164, account.getUuid(), E164_AND_UUID_EXPERIMENT_NAME));

    // test fallback from UUID enrollment to general enrollment percentage
    when(uuidSelector.getUuids()).thenReturn(Collections.emptySet());
    when(experimentEnrollmentConfiguration.getEnrollmentPercentage()).thenReturn(100);
    assertTrue(experimentEnrollmentManager.isEnrolled(NOT_ENROLLED_164, account.getUuid(), E164_AND_UUID_EXPERIMENT_NAME));
    assertTrue(experimentEnrollmentManager.isEnrolled(ENROLLED_164, account.getUuid(), E164_AND_UUID_EXPERIMENT_NAME));

    // test fallback from UUID/general enrollment to e164 enrollment
    when(experimentEnrollmentConfiguration.getEnrollmentPercentage()).thenReturn(0);
    assertTrue(experimentEnrollmentManager.isEnrolled(ENROLLED_164, account.getUuid(), E164_AND_UUID_EXPERIMENT_NAME));
    assertFalse(experimentEnrollmentManager.isEnrolled(NOT_ENROLLED_164, account.getUuid(), E164_AND_UUID_EXPERIMENT_NAME));
    when(e164ExperimentEnrollmentConfiguration.getEnrollmentPercentage()).thenReturn(100);
    assertTrue(experimentEnrollmentManager.isEnrolled(ENROLLED_164, account.getUuid(), E164_AND_UUID_EXPERIMENT_NAME));
    assertTrue(experimentEnrollmentManager.isEnrolled(NOT_ENROLLED_164, account.getUuid(), E164_AND_UUID_EXPERIMENT_NAME));
  }

  @ParameterizedTest
  @MethodSource
  void testIsEnrolled_E164Experiment(final String e164, final String experimentName,
      final Set<String> enrolledE164s, final Set<String> excludedE164s, final Set<String> includedCountryCodes,
      final Set<String> excludedCountryCodes,
      final int enrollmentPercentage,
      final boolean expectEnrolled, final String message) {

    when(e164ExperimentEnrollmentConfiguration.getEnrolledE164s()).thenReturn(enrolledE164s);
    when(e164ExperimentEnrollmentConfiguration.getExcludedE164s()).thenReturn(excludedE164s);
    when(e164ExperimentEnrollmentConfiguration.getEnrollmentPercentage()).thenReturn(enrollmentPercentage);
    when(e164ExperimentEnrollmentConfiguration.getIncludedCountryCodes()).thenReturn(includedCountryCodes);
    when(e164ExperimentEnrollmentConfiguration.getExcludedCountryCodes()).thenReturn(excludedCountryCodes);

    assertEquals(expectEnrolled, experimentEnrollmentManager.isEnrolled(e164, experimentName), message);
  }

  @SuppressWarnings("unused")
  static Stream<Arguments> testIsEnrolled_E164Experiment() {
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
