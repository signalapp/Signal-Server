package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.time.Duration;

public class RedisConnectionPoolConfiguration {

    @JsonProperty
    @Min(1)
    private int poolSize = 16;

    @JsonProperty
    @NotNull
    private Duration maxWait = Duration.ofSeconds(10);

    public int getPoolSize() {
        return poolSize;
    }

    public Duration getMaxWait() {
        return maxWait;
    }
}
