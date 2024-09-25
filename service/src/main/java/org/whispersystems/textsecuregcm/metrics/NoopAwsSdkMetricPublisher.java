package org.whispersystems.textsecuregcm.metrics;

import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;

public class NoopAwsSdkMetricPublisher implements MetricPublisher {

  @Override
  public void publish(final MetricCollection metricCollection) {
  }

  @Override
  public void close() {
  }
}
