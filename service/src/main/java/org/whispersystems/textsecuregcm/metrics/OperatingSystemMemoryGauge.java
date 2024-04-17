/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static org.whispersystems.textsecuregcm.metrics.MetricsUtil.name;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.MeterBinder;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class OperatingSystemMemoryGauge implements MeterBinder {

  private final String metricName;

  private static final File MEMINFO_FILE = new File("/proc/meminfo");
  private static final Pattern MEMORY_METRIC_PATTERN = Pattern.compile("^([^:]+):\\s+([0-9]+).*$");

  public OperatingSystemMemoryGauge(final String metricName) {
    this.metricName = metricName;
  }

  @Override
  public void bindTo(MeterRegistry registry) {
    final String metricName = this.metricName;
    Gauge.builder(name(OperatingSystemMemoryGauge.class, metricName.toLowerCase(Locale.ROOT)), () -> {
          try (final BufferedReader bufferedReader = new BufferedReader(new FileReader(MEMINFO_FILE))) {
            return getValue(bufferedReader.lines(), metricName);
          } catch (final IOException e) {
            return 0L;
          }
        })
        .register(registry);
  }

  @VisibleForTesting
  static double getValue(final Stream<String> lines, final String metricName) {
    return lines.map(MEMORY_METRIC_PATTERN::matcher)
        .filter(Matcher::matches)
        .filter(matcher -> metricName.equalsIgnoreCase(matcher.group(1)))
        .map(matcher -> Double.parseDouble(matcher.group(2)))
        .findFirst()
        .orElse(0d);
  }
}
