/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import javax.validation.constraints.NotNull;
import java.net.URI;
import java.net.UnknownHostException;

import io.dropwizard.metrics.BaseReporterFactory;

@JsonTypeName("json")
public class JsonMetricsReporterFactory extends BaseReporterFactory {

  @JsonProperty
  @NotNull
  private URI uri;

  @Override
  public ScheduledReporter build(MetricRegistry metricRegistry) {
    try {
      return JsonMetricsReporter.forRegistry(metricRegistry)
                                .withUri(uri)
                                .convertRatesTo(getRateUnit())
                                .convertDurationsTo(getDurationUnit())
                                .filter(getFilter())
                                .disabledMetricAttributes(getDisabledAttributes())
                                .build();
    } catch (UnknownHostException e) {
      throw new IllegalArgumentException(e);
    }
  }
}
