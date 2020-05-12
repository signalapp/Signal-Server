package org.whispersystems.textsecuregcm.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.micrometer.wavefront.WavefrontConfig;

import javax.validation.constraints.NotEmpty;

public class MicrometerConfiguration implements WavefrontConfig {

    @JsonProperty
    @NotEmpty
    private String uri;

    @JsonProperty
    @NotEmpty
    private String apiToken;

    @Override
    public String get(final String key) {
        return null;
    }

    @Override
    public String uri() {
        return uri;
    }

    @Override
    public String apiToken() {
        return apiToken;
    }
}
