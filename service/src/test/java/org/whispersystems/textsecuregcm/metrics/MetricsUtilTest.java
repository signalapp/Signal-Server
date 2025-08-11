/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.cumulative.CumulativeDistributionSummary;
import io.micrometer.core.instrument.distribution.CountAtBucket;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.registry.otlp.OtlpMeterRegistry;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import org.assertj.core.api.AbstractStringAssert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.whispersystems.textsecuregcm.configuration.OpenTelemetryConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicMetricsConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;


class MetricsUtilTest {

  @Test
  void name() {

    assertEquals("chat.MetricsUtilTest.metric", MetricsUtil.name(MetricsUtilTest.class, "metric"));
    assertEquals("chat.MetricsUtilTest.namespace.metric",
        MetricsUtil.name(MetricsUtilTest.class, "namespace", "metric"));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void lettuceTagRejection(final boolean enableLettuceRemoteTag) {
    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    final DynamicMetricsConfiguration metricsConfiguration = new DynamicMetricsConfiguration(enableLettuceRemoteTag, false);
    when(dynamicConfiguration.getMetricsConfiguration()).thenReturn(metricsConfiguration);
    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);
    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);

    final MeterRegistry registry = new SimpleMeterRegistry();
    MetricsUtil.configureMeterFilters(registry.config(), dynamicConfigurationManager);

    registry.counter("lettuce.command.completion.max", "command", "hello", "remote", "world", "allowed", "!").increment();
    final List<Meter> meters = registry.getMeters();
    assertThat(meters).hasSize(1);

    final Meter meter = meters.getFirst();
    assertThat(meter.getId().getName()).isEqualTo("chat.lettuce.command.completion.max");
    assertThat(meter.getId().getTag("command")).isNull();
    AbstractStringAssert<?> remoteTag = assertThat(meter.getId().getTag("remote"));

    if (enableLettuceRemoteTag) {
      remoteTag.isNotNull();
    } else {
      remoteTag.isNull();
    }
    assertThat(meter.getId().getTag("allowed")).isNotNull();
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void awsSdkMetricRejection(final boolean enableAwsSdkMetrics) {
    @SuppressWarnings("unchecked") final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager =
        mock(DynamicConfigurationManager.class);

    final DynamicConfiguration dynamicConfiguration = mock(DynamicConfiguration.class);
    final DynamicMetricsConfiguration metricsConfiguration = new DynamicMetricsConfiguration(false, enableAwsSdkMetrics);

    when(dynamicConfigurationManager.getConfiguration()).thenReturn(dynamicConfiguration);
    when(dynamicConfiguration.getMetricsConfiguration()).thenReturn(metricsConfiguration);

    final MeterRegistry registry = new SimpleMeterRegistry();
    MetricsUtil.configureMeterFilters(registry.config(), dynamicConfigurationManager);
    registry.counter("chat.MicrometerAwsSdkMetricPublisher.days_since_last_incident").increment();

    assertThat(registry.getMeters()).hasSize(enableAwsSdkMetrics ? 1 : 0);
  }

  @Test
  void exponentialHistogramBuckets() {
    final OpenTelemetryConfiguration openTelemetryConfig =
      new OpenTelemetryConfiguration(true, Duration.ZERO, 8, Map.of(), Map.of());

    // Yes, this has to be OtlpMeterRegistry—its histograms behave differently than SimpleMeterRegistry's 😭
    final MeterRegistry registry = new OtlpMeterRegistry(openTelemetryConfig, Clock.SYSTEM);

    MetricsUtil.configureHistogramFilters(registry.config(), openTelemetryConfig);
    final DistributionSummary summary = DistributionSummary.builder("test.distribution")
        .publishPercentileHistogram()
        .minimumExpectedValue(1.0)
        .maximumExpectedValue(64.0)
        .register(registry);

    IntStream.range(1, 128).forEach(summary::record);

    final CountAtBucket[] counts = summary.takeSnapshot().histogramCounts();
    assertThat(counts).containsExactly(
      new CountAtBucket(1.0, 1),
      new CountAtBucket(2.0, 1),
      new CountAtBucket(4.0, 2),
      new CountAtBucket(8.0, 4),
      new CountAtBucket(16.0, 8),
      new CountAtBucket(32.0, 16),
      new CountAtBucket(64.0, 32),
      new CountAtBucket(Double.POSITIVE_INFINITY, 63));
  }

}
