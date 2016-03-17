package org.whispersystems.textsecuregcm.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metered;
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

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class JsonMetricsReporter extends ScheduledReporter {

  private static final Pattern SIMPLE_NAMES = Pattern.compile("[^a-zA-Z0-9_.\\-~]");

  private final Logger logger  = LoggerFactory.getLogger(JsonMetricsReporter.class);
  private final JsonFactory factory = new JsonFactory();

  private final String token;
  private final String hostname;
  private final String host;

  public JsonMetricsReporter(MetricRegistry registry, String token, String hostname,
                             MetricFilter filter, TimeUnit rateUnit, TimeUnit durationUnit)
      throws UnknownHostException
  {
    super(registry, "json-reporter", filter, rateUnit, durationUnit);
    this.token    = token;
    this.hostname = hostname;
    this.host     = InetAddress.getLocalHost().getHostName();
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
      URL url = new URL("https", hostname, 443, String.format("/report/metrics?t=%s&h=%s", token, host));
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();

      connection.setDoOutput(true);
      connection.addRequestProperty("Content-Type", "application/json");

      OutputStream  outputStream = connection.getOutputStream();
      JsonGenerator json         = factory.createGenerator(outputStream, JsonEncoding.UTF8);

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

      logger.debug("Metrics server response: " + connection.getResponseCode());
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
    json.writeNumberField("max", convertDuration(snapshot.getMax()));
    json.writeNumberField("mean", convertDuration(snapshot.getMean()));
    json.writeNumberField("min", convertDuration(snapshot.getMin()));
    json.writeNumberField("stddev", convertDuration(snapshot.getStdDev()));
    json.writeNumberField("median", convertDuration(snapshot.getMedian()));
    json.writeNumberField("p75", convertDuration(snapshot.get75thPercentile()));
    json.writeNumberField("p95", convertDuration(snapshot.get95thPercentile()));
    json.writeNumberField("p98", convertDuration(snapshot.get98thPercentile()));
    json.writeNumberField("p99", convertDuration(snapshot.get99thPercentile()));
    json.writeNumberField("p999", convertDuration(snapshot.get999thPercentile()));
  }

  private void writeSnapshot(JsonGenerator json, Snapshot snapshot) throws IOException {
    json.writeNumberField("max", snapshot.getMax());
    json.writeNumberField("mean", snapshot.getMean());
    json.writeNumberField("min", snapshot.getMin());
    json.writeNumberField("stddev", snapshot.getStdDev());
    json.writeNumberField("median", snapshot.getMedian());
    json.writeNumberField("p75", snapshot.get75thPercentile());
    json.writeNumberField("p95", snapshot.get95thPercentile());
    json.writeNumberField("p98", snapshot.get98thPercentile());
    json.writeNumberField("p99", snapshot.get99thPercentile());
    json.writeNumberField("p999", snapshot.get999thPercentile());
  }

  private void writeMetered(JsonGenerator json, Metered meter) throws IOException {
    json.writeNumberField("count", convertRate(meter.getCount()));
    json.writeNumberField("mean", convertRate(meter.getMeanRate()));
    json.writeNumberField("m1", convertRate(meter.getOneMinuteRate()));
    json.writeNumberField("m5", convertRate(meter.getFiveMinuteRate()));
    json.writeNumberField("m15", convertRate(meter.getFifteenMinuteRate()));
  }

  private String sanitize(String metricName) {
    return SIMPLE_NAMES.matcher(metricName).replaceAll("_");
  }

  public static Builder forRegistry(MetricRegistry registry) {
    return new Builder(registry);
  }

  public static class Builder {

    private final MetricRegistry registry;
    private       MetricFilter   filter       = MetricFilter.ALL;
    private       TimeUnit       rateUnit     = TimeUnit.SECONDS;
    private       TimeUnit       durationUnit = TimeUnit.MILLISECONDS;
    private       String         token;
    private       String         hostname;

    private Builder(MetricRegistry registry) {
      this.registry     = registry;
      this.rateUnit     = TimeUnit.SECONDS;
      this.durationUnit = TimeUnit.MILLISECONDS;
      this.filter       = MetricFilter.ALL;
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

    public Builder withToken(String token) {
      this.token = token;
      return this;
    }

    public Builder withHostname(String hostname) {
      this.hostname = hostname;
      return this;
    }

    public JsonMetricsReporter build() throws UnknownHostException {
      if (hostname == null) {
        throw new IllegalArgumentException("No hostname specified!");
      }

      if (token == null) {
        throw new IllegalArgumentException("No token specified!");
      }

      return new JsonMetricsReporter(registry, token, hostname, filter, rateUnit, durationUnit);
    }
  }
}