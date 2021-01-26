/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Test;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.*;

public class DynamicConfigurationTest {

    @Test
    public void testParseExperimentConfig() throws JsonProcessingException {
        {
            final String emptyConfigYaml = "test: true";
            final DynamicConfiguration emptyConfig = DynamicConfigurationManager.OBJECT_MAPPER.readValue(emptyConfigYaml, DynamicConfiguration.class);

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

            final DynamicConfiguration config = DynamicConfigurationManager.OBJECT_MAPPER.readValue(experimentConfigYaml, DynamicConfiguration.class);

            assertFalse(config.getExperimentEnrollmentConfiguration("unconfigured").isPresent());

            assertTrue(config.getExperimentEnrollmentConfiguration("percentageOnly").isPresent());
            assertEquals(12, config.getExperimentEnrollmentConfiguration("percentageOnly").get().getEnrollmentPercentage());
            assertEquals(Collections.emptySet(), config.getExperimentEnrollmentConfiguration("percentageOnly").get().getEnrolledUuids());

            assertTrue(config.getExperimentEnrollmentConfiguration("uuidsAndPercentage").isPresent());
            assertEquals(77, config.getExperimentEnrollmentConfiguration("uuidsAndPercentage").get().getEnrollmentPercentage());
            assertEquals(Set.of(UUID.fromString("717b1c09-ed0b-4120-bb0e-f4697534b8e1"), UUID.fromString("279f264c-56d7-4bbf-b9da-de718ff90903")),
                         config.getExperimentEnrollmentConfiguration("uuidsAndPercentage").get().getEnrolledUuids());

            assertTrue(config.getExperimentEnrollmentConfiguration("uuidsOnly").isPresent());
            assertEquals(0, config.getExperimentEnrollmentConfiguration("uuidsOnly").get().getEnrollmentPercentage());
            assertEquals(Set.of(UUID.fromString("71618739-114c-4b1f-bb0d-6478a44eb600")),
                         config.getExperimentEnrollmentConfiguration("uuidsOnly").get().getEnrolledUuids());
        }
    }
}
