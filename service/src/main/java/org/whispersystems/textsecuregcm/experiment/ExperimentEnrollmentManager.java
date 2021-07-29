/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.experiment;

import java.util.Optional;
import java.util.UUID;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicExperimentEnrollmentConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicPreRegistrationExperimentEnrollmentConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.Util;

public class ExperimentEnrollmentManager {

  private final DynamicConfigurationManager dynamicConfigurationManager;

  public ExperimentEnrollmentManager(final DynamicConfigurationManager dynamicConfigurationManager) {
    this.dynamicConfigurationManager = dynamicConfigurationManager;
  }

  public boolean isEnrolled(final UUID accountUuid, final String experimentName) {

    final Optional<DynamicExperimentEnrollmentConfiguration> maybeConfiguration = dynamicConfigurationManager
        .getConfiguration().getExperimentEnrollmentConfiguration(experimentName);

    return maybeConfiguration.map(config -> {

      if (config.getEnrolledUuids().contains(accountUuid)) {
        return true;
      }

      return isEnrolled(accountUuid, config.getEnrollmentPercentage(), experimentName);

    }).orElse(false);
  }

  public boolean isEnrolled(final String e164, final String experimentName) {

    final Optional<DynamicPreRegistrationExperimentEnrollmentConfiguration> maybeConfiguration = dynamicConfigurationManager
        .getConfiguration().getPreRegistrationEnrollmentConfiguration(experimentName);

    return maybeConfiguration.map(config -> {

      if (config.getEnrolledE164s().contains(e164)) {
        return true;
      }

      if (config.getExcludedE164s().contains(e164)) {
        return false;
      }

      {
        final String countryCode = Util.getCountryCode(e164);

        if (config.getIncludedCountryCodes().contains(countryCode)) {
          return true;
        }

        if (config.getExcludedCountryCodes().contains(countryCode)) {
          return false;
        }
      }

      return isEnrolled(e164, config.getEnrollmentPercentage(), experimentName);

    }).orElse(false);
  }

  private boolean isEnrolled(final Object entity, final int enrollmentPercentage, final String experimentName) {
    final int enrollmentHash = ((entity.hashCode() ^ experimentName.hashCode()) & Integer.MAX_VALUE) % 100;

    return enrollmentHash < enrollmentPercentage;
  }
}
