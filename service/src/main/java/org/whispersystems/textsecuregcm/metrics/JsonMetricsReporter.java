/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
import com.codahale.metrics.MetricAttribute;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

public class JsonMetricsReporter extends ScheduledReporter {

  private static final Pattern SIMPLE_NAMES = Pattern.compile("[^a-zA-Z0-9_.\\-~]");

  private final Logger logger  = LoggerFactory.getLogger(JsonMetricsReporter.class);
  private final JsonFactory factory = new JsonFactory();

  private final URI        uri;
  private final HttpClient httpClient;

  /**
   * A simple named thread factory, copied shamelessly from ScheduledReporter (where it's private).
   */
  private static class NamedThreadFactory implements ThreadFactory {
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    private NamedThreadFactory(String name) {
      final SecurityManager s = System.getSecurityManager();
      this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
      this.namePrefix = "metrics-" + name + "-thread-";
    }

    @Override
    public Thread newThread(Runnable r) {
      final Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
      t.setDaemon(true);
      if (t.getPriority() != Thread.NORM_PRIORITY) {
        t.setPriority(Thread.NORM_PRIORITY);
      }
      return t;
    }
  }

  public JsonMetricsReporter(MetricRegistry registry, URI uri,
                             MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit,
                             Set<MetricAttribute> disabledMetricAttributes)
      throws UnknownHostException
  {
    super(registry, "json-reporter", filter, rateUnit, durationUnit, Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("json-reporter")), true, disabledMetricAttributes);
    this.httpClient = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build();
    this.uri        = UriBuilder.fromUri(uri).queryParam("h", InetAddress.getLocalHost().getHostName()).build();
  }

  @Override
  public void report(SortedMap<String, Gauge>     stringGaugeSortedMap,
                     SortedMap<String, Counter>   stringCounterSortedMap,
                     SortedMap<String, Histogram> stringHistogramSortedMap,
                     SortedMap<String, Meter>     stringMeterSortedMap,
                     SortedMap<String, Timer>     stringTimerSortedMap)
  {
    try {
      logger.debug("Reporting metrics...");

      ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
      JsonGenerator         json         = factory.createGenerator(outputStream, JsonEncoding.UTF8);

      json.writeStartObject();

      for (Map.Entry<String, Gauge> gauge : stringGaugeSortedMap.entrySet()) {
        reportGauge(json, gauge.getKey(), gauge.getValue());
      }

      for (Map.Entry<String, Counter> counter : stringCounterSortedMap.entrySet()) {
        reportCounter(json, counter.getKey(), counter.getValue());
      }

      for (Map.Entry<String, Histogram> histogram : stringHistogramSortedMap.entrySet()) {
        reportHistogram(json, histogram.getKey(), histogram.getValue());
      }

      for (Map.Entry<String, Meter> meter : stringMeterSortedMap.entrySet()) {
        reportMeter(json, meter.getKey(), meter.getValue());
      }

      for (Map.Entry<String, Timer> timer : stringTimerSortedMap.entrySet()) {
        reportTimer(json, timer.getKey(), timer.getValue());
      }

      json.writeEndObject();
      json.close();

      outputStream.close();

      HttpRequest request = HttpRequest.newBuilder()
                                       .uri(uri)
                                       .POST(HttpRequest.BodyPublishers.ofByteArray(outputStream.toByteArray()))
                                       .header("Content-Type", "application/json")
                                       .build();

      HttpResponse<Void> response = httpClient.send(request, HttpResponse.BodyHandlers.discarding());

      logger.debug("Metrics server response: " + response.statusCode());
    } catch (IOException e) {
      logger.warn("Error sending metrics", e);
    } catch (Exception e) {
      logger.warn("error", e);
    }
  }

  private void reportGauge(JsonGenerator json, String name, Gauge gauge) throws IOException {
    Object gaugeValue = evaluateGauge(gauge);

    if (gaugeValue instanceof Number) {
      json.writeFieldName(sanitize(name));
      json.writeObject(gaugeValue);
    }
  }

  private void reportCounter(JsonGenerator json, String name, Counter counter) throws IOException {
    json.writeFieldName(sanitize(name));
    json.writeNumber(counter.getCount());
  }

  private void reportHistogram(JsonGenerator json, String name, Histogram histogram) throws IOException {
    Snapshot snapshot = histogram.getSnapshot();
    json.writeFieldName(sanitize(name));
    json.writeStartObject();
    json.writeNumberField("count", histogram.getCount());
    writeSnapshot(json, snapshot);
    json.writeEndObject();
  }

  private void reportMeter(JsonGenerator json, String name, Meter meter) throws IOException {
    json.writeFieldName(sanitize(name));
    json.writeStartObject();
    writeMetered(json, meter);
    json.writeEndObject();
  }

  private void reportTimer(JsonGenerator json, String name, Timer timer) throws IOException {
    json.writeFieldName(sanitize(name));
    json.writeStartObject();
    json.writeFieldName("rate");
    json.writeStartObject();
    writeMetered(json, timer);
    json.writeEndObject();
    json.writeFieldName("duration");
    json.writeStartObject();
    writeTimedSnapshot(json, timer.getSnapshot());
    json.writeEndObject();
    json.writeEndObject();
  }

  private Object evaluateGauge(Gauge gauge) {
    try {
      return gauge.getValue();
    } catch (RuntimeException e) {
      logger.warn("Error reading gauge", e);
      return "error reading gauge";
    }
  }

  private void writeTimedSnapshot(JsonGenerator json, Snapshot snapshot) throws IOException {
    if (!getDisabledMetricAttributes().contains(MetricAttribute.MAX)) {
      json.writeNumberField("max", convertDuration(snapshot.getMax()));
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.MEAN)) {
      json.writeNumberField("mean", convertDuration(snapshot.getMean()));
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.MIN)) {
      json.writeNumberField("min", convertDuration(snapshot.getMin()));
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.STDDEV)) {
      json.writeNumberField("stddev", convertDuration(snapshot.getStdDev()));
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.P50)) {
      json.writeNumberField("median", convertDuration(snapshot.getMedian()));
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.P75)) {
      json.writeNumberField("p75", convertDuration(snapshot.get75thPercentile()));
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.P95)) {
      json.writeNumberField("p95", convertDuration(snapshot.get95thPercentile()));
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.P98)) {
      json.writeNumberField("p98", convertDuration(snapshot.get98thPercentile()));
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.P99)) {
      json.writeNumberField("p99", convertDuration(snapshot.get99thPercentile()));
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.P999)) {
      json.writeNumberField("p999", convertDuration(snapshot.get999thPercentile()));
    }
  }

  private void writeSnapshot(JsonGenerator json, Snapshot snapshot) throws IOException {
    if (!getDisabledMetricAttributes().contains(MetricAttribute.MAX)) {
      json.writeNumberField("max", snapshot.getMax());
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.MEAN)) {
      json.writeNumberField("mean", snapshot.getMean());
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.MIN)) {
      json.writeNumberField("min", snapshot.getMin());
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.STDDEV)) {
      json.writeNumberField("stddev", snapshot.getStdDev());
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.P50)) {
      json.writeNumberField("median", snapshot.getMedian());
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.P75)) {
      json.writeNumberField("p75", snapshot.get75thPercentile());
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.P95)) {
      json.writeNumberField("p95", snapshot.get95thPercentile());
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.P98)) {
      json.writeNumberField("p98", snapshot.get98thPercentile());
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.P99)) {
      json.writeNumberField("p99", snapshot.get99thPercentile());
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.P999)) {
      json.writeNumberField("p999", snapshot.get999thPercentile());
    }
  }

  private void writeMetered(JsonGenerator json, Metered meter) throws IOException {
    if (!getDisabledMetricAttributes().contains(MetricAttribute.COUNT)) {
      json.writeNumberField("count", convertRate(meter.getCount()));
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.MEAN_RATE)) {
      json.writeNumberField("mean", convertRate(meter.getMeanRate()));
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.M1_RATE)) {
      json.writeNumberField("m1", convertRate(meter.getOneMinuteRate()));
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.M5_RATE)) {
      json.writeNumberField("m5", convertRate(meter.getFiveMinuteRate()));
    }

    if (!getDisabledMetricAttributes().contains(MetricAttribute.M15_RATE)) {
      json.writeNumberField("m15", convertRate(meter.getFifteenMinuteRate()));
    }
  }

  private String sanitize(String metricName) {
    return SIMPLE_NAMES.matcher(metricName).replaceAll("_");
  }

  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  public static class Builder {

    private final MetricRegistry       registry;
    private       MetricFilter         filter                   = MetricFilter.ALL;
    private       Set<MetricAttribute> disabledMetricAttributes = Collections.emptySet();
    private       TimeUnit             rateUnit                 = TimeUnit.SECONDS;
    private       TimeUnit             durationUnit             = TimeUnit.MILLISECONDS;
    private       URI                  uri;

    private Builder(MetricRegistry registry) {
      this.registry = registry;
    }

    public Builder convertRatesTo(TimeUnit rateUnit) {
      this.rateUnit = rateUnit;
      return this;
    }

    public Builder convertDurationsTo(TimeUnit durationUnit) {
      this.durationUnit = durationUnit;
      return this;
    }

    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    public Builder disabledMetricAttributes(Set<MetricAttribute> disabledMetricAttributes) {
      this.disabledMetricAttributes = disabledMetricAttributes;
      return this;
    }

    public Builder withUri(URI uri) {
      this.uri = uri;
      return this;
    }

    public JsonMetricsReporter build() throws UnknownHostException {
      if (uri == null) {
        throw new IllegalArgumentException("No URI specified!");
      }

      return new JsonMetricsReporter(registry, uri, filter, rateUnit, durationUnit, disabledMetricAttributes);
    }
  }
}
