/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.core.setup.Environment;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.binder.jetty.JettySslHandshakeMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.registry.otlp.HistogramFlavor;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.micrometer.statsd.StatsdMeterRegistry;
import java.time.Duration;
import java.util.Optional;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.WhisperServerVersion;
import org.whispersystems.textsecuregcm.configuration.OpenTelemetryConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.Constants;

public class MetricsUtil {

  private static final Logger log = LoggerFactory.getLogger(MetricsUtil.class);

  public static final String PREFIX = "chat";

  private static volatile boolean registeredMetrics = false;

  /**
   * Returns a dot-separated ('.') name for the given class and name parts
   */
  public static String name(Class<?> clazz, String... parts) {
    return name(clazz.getSimpleName(), parts);
  }

  private static String name(String name, String... parts) {
    final StringBuilder sb = new StringBuilder(PREFIX);
    sb.append(".").append(name);
    for (String part : parts) {
      sb.append(".").append(part);
    }
    return sb.toString();
  }

  public static void configureRegistries(final WhisperServerConfiguration config, final Environment environment,
      DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {

    if (registeredMetrics) {
      throw new IllegalStateException("Metric registries configured more than once");
    }

    registeredMetrics = true;

    SharedMetricRegistries.add(Constants.METRICS_NAME, environment.metrics());

    Duration shutdownWaitDuration = Duration.ZERO;

    if (config.getDatadogConfiguration().enabled()) {
      final StatsdMeterRegistry dogstatsdMeterRegistry = new StatsdMeterRegistry(
          config.getDatadogConfiguration(), io.micrometer.core.instrument.Clock.SYSTEM);

      dogstatsdMeterRegistry.config().commonTags(
          Tags.of(
              "service", "chat",
              "version", WhisperServerVersion.getServerVersion(),
              "env", config.getDatadogConfiguration().getEnvironment()));

      configureMeterFilters(dogstatsdMeterRegistry.config(), dynamicConfigurationManager);
      Metrics.addRegistry(dogstatsdMeterRegistry);

      shutdownWaitDuration = config.getDatadogConfiguration().getShutdownWaitDuration();
    }

    if (config.getOpenTelemetryConfiguration().enabled()) {
      final OtlpMeterRegistry otlpMeterRegistry = new OtlpMeterRegistry(
        config.getOpenTelemetryConfiguration(), io.micrometer.core.instrument.Clock.SYSTEM);

      configureMeterFilters(otlpMeterRegistry.config(), dynamicConfigurationManager);
      configureHistogramFilters(otlpMeterRegistry.config(), config.getOpenTelemetryConfiguration());
      Metrics.addRegistry(otlpMeterRegistry);

      if (config.getOpenTelemetryConfiguration().shutdownWaitDuration().compareTo(shutdownWaitDuration) > 0) {
        shutdownWaitDuration = config.getOpenTelemetryConfiguration().shutdownWaitDuration();
      }
    }

    environment.lifecycle().addServerLifecycleListener(
        server -> JettySslHandshakeMetrics.addToAllConnectors(server, Metrics.globalRegistry));

    environment.lifecycle().addEventListener(new ApplicationShutdownMonitor(Metrics.globalRegistry));
    environment.lifecycle().addEventListener(
        new MicrometerRegistryManager(Metrics.globalRegistry, shutdownWaitDuration));
  }

  @VisibleForTesting
  static void configureMeterFilters(MeterRegistry.Config config,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    final DistributionStatisticConfig defaultDistributionStatisticConfig = DistributionStatisticConfig.builder()
        .percentiles(.75, .95, .99, .999)
        .build();

    final String awsSdkMetricNamePrefix = MetricsUtil.name(MicrometerAwsSdkMetricPublisher.class);

    config.meterFilter(new MeterFilter() {
          @Override
          public DistributionStatisticConfig configure(final Meter.Id id, final DistributionStatisticConfig config) {
            return Optional.ofNullable(config.isPercentileHistogram()).orElse(false) ? config : defaultDistributionStatisticConfig.merge(config);
          }
        })
        // Remove high-cardinality `command` tags from Lettuce metrics and prepend "chat." to meter names
        .meterFilter(new MeterFilter() {
          @Override
          public Meter.Id map(final Meter.Id id) {
            if (id.getName().startsWith("lettuce")) {
              return id.withName(PREFIX + "." + id.getName())
                  .replaceTags(id.getTags().stream()
                      .filter(tag -> !"command".equals(tag.getKey()))
                      .filter(tag -> dynamicConfigurationManager.getConfiguration().getMetricsConfiguration().
                          enableLettuceRemoteTag() || !"remote".equals(tag.getKey()))
                      .toList());
            }

            return MeterFilter.super.map(id);
          }
        })
        .meterFilter(MeterFilter.denyNameStartsWith(MessageMetrics.DELIVERY_LATENCY_TIMER_NAME + ".percentile"))
        .meterFilter(MeterFilter.deny(id -> !dynamicConfigurationManager.getConfiguration().getMetricsConfiguration().enableAwsSdkMetrics()
            && id.getName().startsWith(awsSdkMetricNamePrefix)));
  }

  @VisibleForTesting
  static void configureHistogramFilters(MeterRegistry.Config config, OpenTelemetryConfiguration openTelemetryConfig) {
    if (openTelemetryConfig.histogramFlavor() != HistogramFlavor.EXPLICIT_BUCKET_HISTOGRAM) {
      // This workaround for Micrometer's awful defaults is only required for explicit bucket histograms.
      return;
    }

    config.meterFilter(new MeterFilter() {
      @Override
      public DistributionStatisticConfig configure(final Meter.Id id, final DistributionStatisticConfig config) {
        if (config.isPercentileHistogram() == null || !config.isPercentileHistogram()) {
          return config;
        }

        if (config.getMinimumExpectedValueAsDouble() == null || config.getMaximumExpectedValueAsDouble() == null) {
          log.error("Distribution {} does not specify lower or upper bounds, not exporting histograms", id.getName());
          return DistributionStatisticConfig.builder()
            .percentilesHistogram(false)
            .build()
            .merge(config);
        }

        final double lowerBound = config.getMinimumExpectedValueAsDouble();
        final double upperBound = config.getMaximumExpectedValueAsDouble();

        final int numBuckets = Optional.ofNullable(openTelemetryConfig.maxBucketsPerMeter().get(id.getName()))
          .orElse(openTelemetryConfig.maxBucketCount());

        // Bucket i covers values from (buckets[i-1], buckets[i]] except the first one which covers (-inf, buckets[0]].
        // A final bucket will automatically be added at positive infinity, so if we want numBuckets total buckets, we
        // need numBuckets - 1 explicit ones; if we want those to have equal ratios between values, and want an explicit
        // bucket for (-inf, lowerBound] as well as an implicit one for (upperBound, inf], the ratio between buckets will be
        // r = (upperBound/lowerBound)^(1/(numBuckets - 2)), so that we have values at lowerBound * r^i
        // for i in [0, numBuckets-2] i.e. numBuckets - 1 values, plus the one at infinity
        final double scale = Math.pow(upperBound / lowerBound, 1.0 / (numBuckets - 2));
        final double[] buckets = IntStream.range(0, numBuckets - 1).mapToDouble(i -> lowerBound * Math.pow(scale, i)).toArray();

        // yes, percentilesHistogram(false)! Otherwise, Micrometer will add its own non-configurable buckets based on an
        // inferior selection algorithm that produces 69(!) buckets for a range from 1ms to 30s and still yields ±25% relative error
        return DistributionStatisticConfig.builder()
            .percentilesHistogram(false)
            .serviceLevelObjectives(buckets)
            .build()
            .merge(config);
      }
    });
  }

  public static void registerSystemResourceMetrics(final Environment environment) {
    new ProcessorMetrics().bindTo(Metrics.globalRegistry);
    new FileDescriptorMetrics().bindTo(Metrics.globalRegistry);

    new JvmMemoryMetrics().bindTo(Metrics.globalRegistry);
    new JvmThreadMetrics().bindTo(Metrics.globalRegistry);

    GarbageCollectionGauges.registerMetrics();
  }

}
