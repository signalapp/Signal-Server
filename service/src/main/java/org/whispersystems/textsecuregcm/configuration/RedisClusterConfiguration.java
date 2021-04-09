/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.Valid;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.List;

public class RedisClusterConfiguration {

    @JsonProperty
    @NotEmpty
    private List<String> urls;

    @JsonProperty
    @NotNull
    private Duration timeout = Duration.ofMillis(3_000);

    @JsonProperty
    @NotNull
    @Valid
    private CircuitBreakerConfiguration circuitBreaker = new CircuitBreakerConfiguration();

    @JsonProperty
    @NotNull
    @Valid
    private RetryConfiguration retry = new RetryConfiguration();

    public List<String> getUrls() {
        return urls;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public CircuitBreakerConfiguration getCircuitBreakerConfiguration() {
        return circuitBreaker;
    }

    public RetryConfiguration getRetryConfiguration() {
        return retry;
    }
}
