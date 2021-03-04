/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import static com.codahale.metrics.MetricRegistry.name;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NstatCounters {

  private final Map<String, Long> networkStatistics = new ConcurrentHashMap<>();

  private static final String[] NSTAT_COMMAND_LINE = new String[] { "nstat", "--zero", "--json", "--noupdate", "--ignore" };
  private static final String[] EXCLUDE_METRIC_NAME_PREFIXES = new String[] { "Icmp", "Udp", "Ip6" };

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
      .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  private static final Logger log = LoggerFactory.getLogger(NstatCounters.class);

  @VisibleForTesting
  static class NetworkStatistics {
    private final Map<String, Long> kernelStatistics;

    @JsonCreator
    private NetworkStatistics(@JsonProperty("kernel") final Map<String, Long> kernelStatistics) {
      this.kernelStatistics = kernelStatistics;
    }

    public Map<String, Long> getKernelStatistics() {
      return kernelStatistics;
    }
  }

  public void registerMetrics(final ScheduledExecutorService refreshService, final Duration refreshInterval) {
    refreshNetworkStatistics();

    networkStatistics.keySet().stream()
        .filter(NstatCounters::shouldIncludeMetric)
        .forEach(metricName -> Metrics.globalRegistry.more().counter(name(getClass(), "kernel", metricName),
            Collections.emptyList(), networkStatistics, statistics -> statistics.get(metricName)));

    refreshService.scheduleAtFixedRate(this::refreshNetworkStatistics,
        refreshInterval.toMillis(), refreshInterval.toMillis(), TimeUnit.MILLISECONDS);
  }

  private void refreshNetworkStatistics() {
    try {
      networkStatistics.putAll(loadNetworkStatistics().getKernelStatistics());
    } catch (final InterruptedException | IOException e) {
      log.warn("Failed to refresh network statistics", e);
    }
  }

  @VisibleForTesting
  static boolean shouldIncludeMetric(final String metricName) {
    for (final String prefix : EXCLUDE_METRIC_NAME_PREFIXES) {
      if (metricName.startsWith(prefix)) {
        return false;
      }
    }

    return true;
  }

  @VisibleForTesting
  static NetworkStatistics loadNetworkStatistics() throws IOException, InterruptedException {
    final Process nstatProcess = Runtime.getRuntime().exec(NSTAT_COMMAND_LINE);

    if (nstatProcess.waitFor() == 0) {
      return OBJECT_MAPPER.readValue(nstatProcess.getInputStream(), NetworkStatistics.class);
    } else {
      throw new IOException("nstat process did not exit normally");
    }
  }
}
