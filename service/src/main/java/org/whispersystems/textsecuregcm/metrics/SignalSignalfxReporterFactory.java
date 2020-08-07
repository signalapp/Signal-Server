package org.whispersystems.textsecuregcm.metrics;

import com.amazonaws.util.EC2MetadataUtils;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.signalfx.codahale.reporter.SignalFxReporter;
import com.signalfx.metrics.auth.StaticAuthToken;
import io.dropwizard.metrics.BaseReporterFactory;

import javax.validation.constraints.NotEmpty;

@JsonTypeName("signalsignalfx")
public class SignalSignalfxReporterFactory extends BaseReporterFactory {

  @JsonProperty
  @NotEmpty
  private String authToken = null;

  @JsonProperty
  @NotEmpty
  private String environment = null;

  public ScheduledReporter build(MetricRegistry registry) {
    return new SignalFxReporter.Builder(registry, new StaticAuthToken(authToken), EC2MetadataUtils.getInstanceId())
            .addDimension("environment", environment)
            .setFilter(getFilter())
            .setDurationUnit(getDurationUnit())
            .setRateUnit(getRateUnit())
            .build();
  }
}
