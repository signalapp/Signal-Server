/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Positive;

public class MicrometerConfiguration {

    @JsonProperty
    private String uri;

    @JsonProperty
    @Positive
    private int batchSize = 10_000;

    public String getUri() {
        return uri;
    }

    public int getBatchSize() {
        return batchSize;
    }
}
