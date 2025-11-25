/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.SharedMetricRegistries;
import com.google.common.annotations.VisibleForTesting;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.lifecycle.Managed;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.binder.jetty.JettySslHandshakeMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.registry.otlp.OtlpMeterRegistry;
import io.opentelemetry.exporter.otlp.http.logs.OtlpHttpLogRecordExporter;
import io.opentelemetry.instrumentation.logback.appender.v1_0.OpenTelemetryAppender;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.logs.SdkLoggerProvider;
import io.opentelemetry.sdk.logs.export.BatchLogRecordProcessor;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.resources.ResourceBuilder;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;

import org.eclipse.jetty.util.component.LifeCycle;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.configuration.dynamic.DynamicConfiguration;
import org.whispersystems.textsecuregcm.storage.DynamicConfigurationManager;
import org.whispersystems.textsecuregcm.util.Constants;

public class MetricsUtil {

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

    if (config.getOpenTelemetryConfiguration().enabled()) {
      final OtlpMeterRegistry otlpMeterRegistry = new OtlpMeterRegistry(
        config.getOpenTelemetryConfiguration(), io.micrometer.core.instrument.Clock.SYSTEM);

      configureMeterFilters(otlpMeterRegistry.config(), dynamicConfigurationManager);
      Metrics.addRegistry(otlpMeterRegistry);

      shutdownWaitDuration = config.getOpenTelemetryConfiguration().shutdownWaitDuration();
    }

    environment.lifecycle().addServerLifecycleListener(
        server -> JettySslHandshakeMetrics.addToAllConnectors(server, Metrics.globalRegistry));

    new ApplicationShutdownMonitor(Metrics.globalRegistry).register();
    environment.lifecycle().addEventListener(
        new MicrometerRegistryManager(Metrics.globalRegistry, shutdownWaitDuration));

    registerSystemResourceMetrics();
  }

  public static void configureLogging(final WhisperServerConfiguration config, final Environment environment) {
    if (!config.getOpenTelemetryConfiguration().enabled()) {
      return;
    }

    final Map<String, String> env = System.getenv();
    final String endpoint =
      Optional.ofNullable(env.get("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"))
        .or(() -> Optional.ofNullable(env.get("OTEL_EXPORTER_OTLP_ENDPOINT")))
        .map(u -> u.endsWith("/v1/logs") ? u : u + "/v1/logs")
        .orElse("http://localhost:4318/v1/logs");

    final ResourceBuilder resource = Resource.builder();
    config.getOpenTelemetryConfiguration().resourceAttributes().forEach((k, v) -> resource.put(k, v));

    final OpenTelemetrySdk openTelemetry =
      OpenTelemetrySdk.builder()
        .setLoggerProvider(
          SdkLoggerProvider.builder()
            .setResource(resource.build())
            .addLogRecordProcessor(
              BatchLogRecordProcessor.builder(
                OtlpHttpLogRecordExporter.builder()
                  .setEndpoint(endpoint)
                  .build()).build())
            .build())
        .build();

    OpenTelemetryAppender.install(openTelemetry);

    environment.lifecycle().addEventListener(new LifeCycle.Listener() {
      @Override
      public void lifeCycleStopped(final LifeCycle event) {
        openTelemetry.close();
      }
    });
  }

  @VisibleForTesting
  static void configureMeterFilters(MeterRegistry.Config config,
      final DynamicConfigurationManager<DynamicConfiguration> dynamicConfigurationManager) {
    final DistributionStatisticConfig defaultDistributionStatisticConfig = DistributionStatisticConfig.builder()
        .percentilesHistogram(true)
        .percentiles(.75, .95, .99, .999)
        .build();

    final String awsSdkMetricNamePrefix = MetricsUtil.name(MicrometerAwsSdkMetricPublisher.class);

    config.meterFilter(new MeterFilter() {
          @Override
          public DistributionStatisticConfig configure(final Meter.Id id, final DistributionStatisticConfig config) {
            return defaultDistributionStatisticConfig.merge(config);
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

  static void registerSystemResourceMetrics() {
    new ProcessorMetrics().bindTo(Metrics.globalRegistry);
    new FileDescriptorMetrics().bindTo(Metrics.globalRegistry);

    new JvmMemoryMetrics().bindTo(Metrics.globalRegistry);
    new JvmThreadMetrics().bindTo(Metrics.globalRegistry);

    GarbageCollectionGauges.registerMetrics();
  }

}
