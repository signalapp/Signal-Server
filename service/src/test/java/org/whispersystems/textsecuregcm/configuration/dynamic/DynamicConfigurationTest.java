/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.vdurmont.semver4j.Semver;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.junit.Test;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.ua.ClientPlatform;

public class DynamicConfigurationTest {

  @Test
  public void testParseExperimentConfig() throws JsonProcessingException {
    {
      final String emptyConfigYaml = "test: true";
      final DynamicConfiguration emptyConfig = DynamicConfigurationManager.OBJECT_MAPPER
          .readValue(emptyConfigYaml, DynamicConfiguration.class);

      assertFalse(emptyConfig.getExperimentEnrollmentConfiguration("test").isPresent());
    }

    {
      final String experimentConfigYaml =
          "experiments:\n" +
              "  percentageOnly:\n" +
              "    enrollmentPercentage: 12\n" +
              "  uuidsAndPercentage:\n" +
              "    enrolledUuids:\n" +
              "      - 717b1c09-ed0b-4120-bb0e-f4697534b8e1\n" +
              "      - 279f264c-56d7-4bbf-b9da-de718ff90903\n" +
              "    enrollmentPercentage: 77\n" +
              "  uuidsOnly:\n" +
              "    enrolledUuids:\n" +
              "      - 71618739-114c-4b1f-bb0d-6478a44eb600";

      final DynamicConfiguration config = DynamicConfigurationManager.OBJECT_MAPPER
          .readValue(experimentConfigYaml, DynamicConfiguration.class);

      assertFalse(config.getExperimentEnrollmentConfiguration("unconfigured").isPresent());

      assertTrue(config.getExperimentEnrollmentConfiguration("percentageOnly").isPresent());
      assertEquals(12, config.getExperimentEnrollmentConfiguration("percentageOnly").get().getEnrollmentPercentage());
      assertEquals(Collections.emptySet(),
          config.getExperimentEnrollmentConfiguration("percentageOnly").get().getEnrolledUuids());

      assertTrue(config.getExperimentEnrollmentConfiguration("uuidsAndPercentage").isPresent());
      assertEquals(77,
          config.getExperimentEnrollmentConfiguration("uuidsAndPercentage").get().getEnrollmentPercentage());
      assertEquals(Set.of(UUID.fromString("717b1c09-ed0b-4120-bb0e-f4697534b8e1"),
          UUID.fromString("279f264c-56d7-4bbf-b9da-de718ff90903")),
          config.getExperimentEnrollmentConfiguration("uuidsAndPercentage").get().getEnrolledUuids());

      assertTrue(config.getExperimentEnrollmentConfiguration("uuidsOnly").isPresent());
      assertEquals(0, config.getExperimentEnrollmentConfiguration("uuidsOnly").get().getEnrollmentPercentage());
      assertEquals(Set.of(UUID.fromString("71618739-114c-4b1f-bb0d-6478a44eb600")),
          config.getExperimentEnrollmentConfiguration("uuidsOnly").get().getEnrolledUuids());
    }
  }

  @Test
  public void testParseRemoteDeprecationConfig() throws JsonProcessingException {
    {
      final String emptyConfigYaml = "test: true";
      final DynamicConfiguration emptyConfig = DynamicConfigurationManager.OBJECT_MAPPER
          .readValue(emptyConfigYaml, DynamicConfiguration.class);

      assertNotNull(emptyConfig.getRemoteDeprecationConfiguration());
    }

    {
      final String experimentConfigYaml =
          "remoteDeprecation:\n" +
              "  minimumVersions:\n" +
              "    IOS: 1.2.3\n" +
              "    ANDROID: 4.5.6\n" +

              "  versionsPendingDeprecation:\n" +
              "    DESKTOP: 7.8.9\n" +

              "  blockedVersions:\n" +
              "    DESKTOP:\n" +
              "      - 1.4.0-beta.2";

      final DynamicConfiguration config = DynamicConfigurationManager.OBJECT_MAPPER
          .readValue(experimentConfigYaml, DynamicConfiguration.class);
      final DynamicRemoteDeprecationConfiguration remoteDeprecationConfiguration = config
          .getRemoteDeprecationConfiguration();

      assertEquals(Map.of(ClientPlatform.IOS, new Semver("1.2.3"), ClientPlatform.ANDROID, new Semver("4.5.6")),
          remoteDeprecationConfiguration.getMinimumVersions());
      assertEquals(Map.of(ClientPlatform.DESKTOP, new Semver("7.8.9")),
          remoteDeprecationConfiguration.getVersionsPendingDeprecation());
      assertEquals(Map.of(ClientPlatform.DESKTOP, Set.of(new Semver("1.4.0-beta.2"))),
          remoteDeprecationConfiguration.getBlockedVersions());
      assertTrue(remoteDeprecationConfiguration.getVersionsPendingBlock().isEmpty());
    }
  }

  @Test
  public void testParseMessageRateConfiguration() throws JsonProcessingException {
    {
      final String emptyConfigYaml = "test: true";
      final DynamicConfiguration emptyConfig = DynamicConfigurationManager.OBJECT_MAPPER
          .readValue(emptyConfigYaml, DynamicConfiguration.class);

      assertFalse(emptyConfig.getMessageRateConfiguration().isEnforceUnsealedSenderRateLimit());
    }

    {
      final String messageRateConfigYaml =
          "messageRate:\n" +
              "  enforceUnsealedSenderRateLimit: true";

      final DynamicConfiguration emptyConfig = DynamicConfigurationManager.OBJECT_MAPPER
          .readValue(messageRateConfigYaml, DynamicConfiguration.class);

      assertTrue(emptyConfig.getMessageRateConfiguration().isEnforceUnsealedSenderRateLimit());
    }
  }

  @Test
  public void testParseFeatureFlags() throws JsonProcessingException {
    {
      final String emptyConfigYaml = "test: true";
      final DynamicConfiguration emptyConfig = DynamicConfigurationManager.OBJECT_MAPPER
          .readValue(emptyConfigYaml, DynamicConfiguration.class);

      assertTrue(emptyConfig.getActiveFeatureFlags().isEmpty());
    }

    {
      final String emptyConfigYaml =
          "featureFlags:\n"
              + "  - testFlag";

      final DynamicConfiguration emptyConfig = DynamicConfigurationManager.OBJECT_MAPPER
          .readValue(emptyConfigYaml, DynamicConfiguration.class);

      assertTrue(emptyConfig.getActiveFeatureFlags().contains("testFlag"));
    }
  }

  @Test
  public void testParseTwilioConfiguration() throws JsonProcessingException {
    {
      final String emptyConfigYaml = "test: true";
      final DynamicConfiguration emptyConfig = DynamicConfigurationManager.OBJECT_MAPPER
          .readValue(emptyConfigYaml, DynamicConfiguration.class);

      assertTrue(emptyConfig.getTwilioConfiguration().getNumbers().isEmpty());
    }

    {
      final String emptyConfigYaml =
          "twilio:\n"
              + "  numbers:\n"
              + "    - 2135551212\n"
              + "    - 2135551313";

      final DynamicTwilioConfiguration config = DynamicConfigurationManager.OBJECT_MAPPER
          .readValue(emptyConfigYaml, DynamicConfiguration.class)
          .getTwilioConfiguration();

      assertEquals(List.of("2135551212", "2135551313"), config.getNumbers());
    }
  }
}
