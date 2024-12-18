package org.whispersystems.textsecuregcm.metrics;

import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Tags;
import io.micrometer.core.instrument.Timer;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import software.amazon.awssdk.metrics.MetricCollection;
import software.amazon.awssdk.metrics.MetricPublisher;
import software.amazon.awssdk.metrics.MetricRecord;

/**
 * A Micrometer AWS SDK metric publisher consumes {@link MetricCollection} instances provided by the AWS SDK when it
 * makes calls to the AWS API and publishes a subset of metrics from each call via the Micrometer metrics facade. A
 * single {@code MicrometerAwsSdkMetricPublisher} should be bound to a single AWS service client instance; publishers
 * should not be assigned to multiple service clients.
 *
 * @see <a href="https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/metrics-list.html">Service client metrics</a>
 */
public class MicrometerAwsSdkMetricPublisher implements MetricPublisher {

  private final ExecutorService recordMetricsExecutorService;

  private final String awsClientName;
  private final AtomicInteger mostRecentMaxConcurrency;

  // Note that these metric collection type names don't seem to appear anywhere in the AWS SDK documentation. The docs
  // also hint at, but don't explicitly call out, the structure of a `MetricCollection` passed to the `#publish(...)`
  // method. Empirically, for each AWS SDK operation, the SDK will call `#publish(...)` with a collection structured as
  // follows:
  //
  // MetricCollection { name = "ApiCall" }
  // +-- MetricRecord { name = "ApiCallDuration" }
  // +-- MetricRecord { name = "ApiCallSuccessful" }
  // +-- ...
  // +-- MetricCollection { name = "ApiCallAttempt" }
  //     +-- MetricRecord { name = "AwsExtendedRequestId" }
  //     +-- MetricRecord { name = "AwsRequestId" }
  //     +-- ...
  //     +-- MetricCollection {name = "HttpClient" }
  //         +-- MetricRecord { name = "AvailableConcurrency" }
  //         +-- MetricRecord { name = "ConcurrencyAcquireDuration" }
  //         +-- ...
  // +-- MetricCollection { name = "ApiCallAttempt" }
  //     +-- MetricRecord { name = "AwsExtendedRequestId" }
  //     +-- MetricRecord { name = "AwsRequestId" }
  //     +-- ...
  //     +-- MetricCollection {name = "HttpClient" }
  //         +-- MetricRecord { name = "AvailableConcurrency" }
  //         +-- MetricRecord { name = "ConcurrencyAcquireDuration" }
  //         +-- ...
  // +-- ...
  //
  // Every `MetricCollection` passed to `#publish(...)` should have a name of "ApiCall," and the "ApiCall" collection
  // will have a fixed collection of named metrics (which ARE documented!). The "ApiCall" collection should have one or
  // more "ApiCallAttempt" `MetricCollection` as children, and each "ApiCallAttempt" collection should have exactly one
  // "HttpClient" `MetricCollection` as a child.
  private static final String METRIC_COLLECTION_TYPE_API_CALL = "ApiCall";
  private static final String METRIC_COLLECTION_TYPE_API_CALL_ATTEMPT = "ApiCallAttempt";
  private static final String METRIC_COLLECTION_TYPE_HTTP_METRICS = "HttpClient";

  private static final String API_CALL_COUNTER_NAME =
      MetricsUtil.name(MicrometerAwsSdkMetricPublisher.class, "apiCall");

  private static final String API_CALL_RETRY_COUNT_DISTRIBUTION_NAME =
      MetricsUtil.name(MicrometerAwsSdkMetricPublisher.class, "apiCallRetries");

  private static final String CHANNEL_ACQUISITION_TIMER_NAME =
      MetricsUtil.name(MicrometerAwsSdkMetricPublisher.class, "acquireChannelDuration");

  private static final String PENDING_CHANNEL_ACQUISITIONS_DISTRIBUTION_NAME =
      MetricsUtil.name(MicrometerAwsSdkMetricPublisher.class, "pendingChannelAcquisitions");

  private static final String CONCURRENT_REQUESTS_DISTRIBUTION_NAME =
      MetricsUtil.name(MicrometerAwsSdkMetricPublisher.class, "concurrentRequests");

  private static final String MAX_CONCURRENCY_GAUGE_NAME =
          MetricsUtil.name(MicrometerAwsSdkMetricPublisher.class, "maxConcurrency");

  private static final String CLIENT_NAME_TAG = "clientName";

  /**
   * Constructs a new metric publisher that uses the given executor service to record metrics and tags metrics with the
   * given client name.
   *
   * @param recordMetricsExecutorService the executor service via which to record metrics
   * @param awsClientName the name of AWS service client to which this publisher is attached
   */
  public MicrometerAwsSdkMetricPublisher(final ExecutorService recordMetricsExecutorService, final String awsClientName) {
    this.recordMetricsExecutorService = recordMetricsExecutorService;
    this.awsClientName = awsClientName;

    mostRecentMaxConcurrency = Metrics.gauge(MAX_CONCURRENCY_GAUGE_NAME,
        Tags.of(CLIENT_NAME_TAG, awsClientName),
        new AtomicInteger(0));
  }

  @Override
  public void publish(final MetricCollection metricCollection) {
    if (METRIC_COLLECTION_TYPE_API_CALL.equals(metricCollection.name())) {
      try {
        recordMetricsExecutorService.submit(() -> recordApiCallMetrics(metricCollection));
      } catch (final RejectedExecutionException ignored) {
        // This can happen if clients make new calls to an upstream service while the server is shutting down
      }
    }
  }

  private void recordApiCallMetrics(final MetricCollection apiCallMetricCollection) {
    if (!apiCallMetricCollection.name().equals(METRIC_COLLECTION_TYPE_API_CALL)) {
      throw new IllegalArgumentException("Unexpected API call metric collection name: " + apiCallMetricCollection.name());
    }

    final Map<String, MetricRecord<?>> metricsByName = toMetricMap(apiCallMetricCollection);

    final Optional<String> maybeAwsServiceId = Optional.ofNullable(metricsByName.get("ServiceId"))
        .map(metricRecord -> (String) metricRecord.value());

    final Optional<String> maybeOperationName = Optional.ofNullable(metricsByName.get("OperationName"))
        .map(metricRecord -> (String) metricRecord.value());

    // Both the service ID and operation name SHOULD always be present, but since the metric collection is unstructured,
    // we don't have any compile-time guarantees and so check that they're both present as a pedantic safety check.
    if (maybeAwsServiceId.isPresent() && maybeOperationName.isPresent()) {
      final String awsServiceId = maybeAwsServiceId.get();
      final String operationName = maybeOperationName.get();

      final boolean success = Optional.ofNullable(metricsByName.get("ApiCallSuccessful"))
          .map(metricRecord -> (boolean) metricRecord.value())
          .orElse(false);

      final int retryCount = Optional.ofNullable(metricsByName.get("RetryCount"))
          .map(metricRecord -> (int) metricRecord.value())
          .orElse(0);

      final Tags tags = Tags.of(
          CLIENT_NAME_TAG, awsClientName,
          "awsServiceId", awsServiceId,
          "operationName", operationName,
          "callSuccess", String.valueOf(success));

      Metrics.counter(API_CALL_COUNTER_NAME, tags).increment();

      DistributionSummary.builder(API_CALL_RETRY_COUNT_DISTRIBUTION_NAME)
          .tags(tags)
          .publishPercentileHistogram(true)
          .register(Metrics.globalRegistry)
          .record(retryCount);

      apiCallMetricCollection.childrenWithName(METRIC_COLLECTION_TYPE_API_CALL_ATTEMPT)
          .forEach(callAttemptMetricCollection -> recordAttemptMetrics(callAttemptMetricCollection, tags));
    }
  }

  private void recordAttemptMetrics(final MetricCollection apiCallAttemptMetricCollection, final Tags callTags) {
    if (!apiCallAttemptMetricCollection.name().equals(METRIC_COLLECTION_TYPE_API_CALL_ATTEMPT)) {
      throw new IllegalArgumentException("Unexpected API call attempt metric collection name: " + apiCallAttemptMetricCollection.name());
    }

    // A "call attempt" metric collection should always have exactly one HTTP metrics child collection, but we have no
    // compiler-level guarantees and so do a pedantic check here just to be safe.
    apiCallAttemptMetricCollection.childrenWithName(METRIC_COLLECTION_TYPE_HTTP_METRICS).findFirst().ifPresent(httpMetricCollection -> {
      final Map<String, MetricRecord<?>> callAttemptMetricsByName = toMetricMap(apiCallAttemptMetricCollection);
      final Map<String, MetricRecord<?>> httpMetricsByName = toMetricMap(httpMetricCollection);

      Optional.ofNullable(httpMetricsByName.get("MaxConcurrency"))
          .ifPresent(maxConcurrencyMetricRecord -> mostRecentMaxConcurrency.set((int) maxConcurrencyMetricRecord.value()));

      final Tags attemptTags = Optional.ofNullable(callAttemptMetricsByName.get("ErrorType"))
          .map(errorTypeMetricRecord -> callTags.and("error", errorTypeMetricRecord.value().toString()))
          .orElse(callTags);

      Optional.ofNullable(httpMetricsByName.get("ConcurrencyAcquireDuration"))
          .ifPresent(channelAcquisitionDurationMetricRecord -> Timer.builder(CHANNEL_ACQUISITION_TIMER_NAME)
              .tags(attemptTags)
              .publishPercentileHistogram(true)
              .register(Metrics.globalRegistry)
              .record((Duration) channelAcquisitionDurationMetricRecord.value()));

      Optional.ofNullable(httpMetricsByName.get("LeasedConcurrency"))
          .ifPresent(concurrentRequestsMetricRecord -> DistributionSummary.builder(CONCURRENT_REQUESTS_DISTRIBUTION_NAME)
              .tags(attemptTags)
              .publishPercentileHistogram(true)
              .register(Metrics.globalRegistry)
              .record((int) concurrentRequestsMetricRecord.value()));

      Optional.ofNullable(httpMetricsByName.get("PendingConcurrencyAcquires"))
          .ifPresent(pendingChannelAcquisitionsMetricRecord -> DistributionSummary.builder(PENDING_CHANNEL_ACQUISITIONS_DISTRIBUTION_NAME)
              .tags(attemptTags)
              .publishPercentileHistogram(true)
              .register(Metrics.globalRegistry)
              .record((int) pendingChannelAcquisitionsMetricRecord.value()));
    });
  }

  private static Map<String, MetricRecord<?>> toMetricMap(final MetricCollection metricCollection) {
    return metricCollection.stream()
        .collect(Collectors.toMap(metricRecord -> metricRecord.metric().name(), metricRecord -> metricRecord));
  }

  @Override
  public void close() {
  }
}
