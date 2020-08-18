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
    private Duration timeout = Duration.ofSeconds(2);

    @JsonProperty
    @NotNull
    @Valid
    private CircuitBreakerConfiguration circuitBreaker = new CircuitBreakerConfiguration();

    public List<String> getUrls() {
        return urls;
    }

    public Duration getTimeout() {
        return timeout;
    }

    public CircuitBreakerConfiguration getCircuitBreakerConfiguration() {
        return circuitBreaker;
    }
}
