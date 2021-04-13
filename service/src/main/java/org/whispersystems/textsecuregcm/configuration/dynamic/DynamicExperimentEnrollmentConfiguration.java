/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration.dynamic;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

public class DynamicExperimentEnrollmentConfiguration {

    @JsonProperty
    @Valid
    private Set<UUID> enrolledUuids = Collections.emptySet();

    @JsonProperty
    @Valid
    @Min(0)
    @Max(100)
    private int enrollmentPercentage = 0;

    public Set<UUID> getEnrolledUuids() {
        return enrolledUuids;
    }

    public int getEnrollmentPercentage() {
        return enrollmentPercentage;
    }
}
