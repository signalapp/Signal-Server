/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import java.time.Duration;

public class DynamoDbConfiguration {

    @JsonProperty
    @NotBlank
    private String region;

    @JsonProperty
    @NotBlank
    private String tableName;

    @JsonProperty
    private Duration clientExecutionTimeout = Duration.ofSeconds(30);

    @JsonProperty
    private Duration clientRequestTimeout = Duration.ofSeconds(10);

    @Valid
    @NotEmpty
    public String getRegion() {
        return region;
    }

    @Valid
    @NotEmpty
    public String getTableName() {
        return tableName;
    }

    public Duration getClientExecutionTimeout() {
        return clientExecutionTimeout;
    }

    public Duration getClientRequestTimeout() {
        return clientRequestTimeout;
    }
}
