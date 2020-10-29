/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotEmpty;

public class MicrometerConfiguration {

    @JsonProperty
    private String uri;

    @JsonProperty
    @NotEmpty
    private String apiKey;

    public String getUri() {
        return uri;
    }

    public String getApiKey() {
        return apiKey;
    }
}
