/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import java.time.Duration;

public class DynamoDbConfiguration {

    private String   region;
    private String   tableName;
    private Duration clientExecutionTimeout = Duration.ofSeconds(30);
    private Duration clientRequestTimeout   = Duration.ofSeconds(10);

    @Valid
    @NotEmpty
    @JsonProperty
    public String getRegion() {
        return region;
    }

    @Valid
    @NotEmpty
    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    @JsonProperty
    public Duration getClientExecutionTimeout() {
        return clientExecutionTimeout;
    }

    @JsonProperty
    public Duration getClientRequestTimeout() {
        return clientRequestTimeout;
    }
}
