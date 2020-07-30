package org.whispersystems.textsecuregcm.metrics;

import com.amazonaws.util.EC2MetadataUtils;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.dropwizard.metrics.BaseReporterFactory;
import org.coursera.metrics.datadog.DatadogReporter;
import org.coursera.metrics.datadog.DefaultMetricNameFormatterFactory;
import org.coursera.metrics.datadog.DynamicTagsCallbackFactory;
import org.coursera.metrics.datadog.MetricNameFormatterFactory;
import org.coursera.metrics.datadog.transport.AbstractTransportFactory;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.EnumSet;
import java.util.List;

@SuppressWarnings("FieldMayBeFinal")
@JsonTypeName("signaldatadog")
public class SignalDatadogReporterFactory extends BaseReporterFactory {

  @JsonProperty
  private List<String> tags = null;

  @Valid
  @JsonProperty
  private DynamicTagsCallbackFactory dynamicTagsCallback = null;

  @JsonProperty
  private String prefix = null;

  @Valid
  @NotNull
  @JsonProperty
  private EnumSet<DatadogReporter.Expansion> expansions = DatadogReporter.Expansion.ALL;

  @Valid
  @NotNull
  @JsonProperty
  private MetricNameFormatterFactory metricNameFormatter = new DefaultMetricNameFormatterFactory();

  @Valid
  @NotNull
  @JsonProperty
  private AbstractTransportFactory transport = null;

  public ScheduledReporter build(MetricRegistry registry) {
    return DatadogReporter.forRegistry(registry)
                          .withTransport(transport.build())
                          .withHost(EC2MetadataUtils.getInstanceId())
                          .withTags(tags)
                          .withPrefix(prefix)
                          .withExpansions(expansions)
                          .withMetricNameFormatter(metricNameFormatter.build())
                          .withDynamicTagCallback(dynamicTagsCallback != null ? dynamicTagsCallback.build() : null)
                          .filter(getFilter())
                          .convertDurationsTo(getDurationUnit())
                          .convertRatesTo(getRateUnit())
                          .build();
  }
}
