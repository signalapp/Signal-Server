package org.whispersystems.textsecuregcm.scheduler;

import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.metrics.MetricsUtil;
import org.whispersystems.textsecuregcm.util.Util;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A job scheduler maintains a delay queue of tasks to be run at some time in the future. Callers schedule jobs with
 * the {@link #scheduleJob(Instant, byte[])} method, and concrete subclasses actually execute jobs by implementing the
 * {@link #processJob(byte[])} method. Some entity must call {@link #processAvailableJobs()} to actually find and
 * process jobs that are ready for execution.
 */
public abstract class JobScheduler {

  private final DynamoDbAsyncClient dynamoDbAsyncClient;
  private final String tableName;
  private final Duration jobExpiration;
  private final Clock clock;

  private final Logger logger = LoggerFactory.getLogger(getClass());

  // The name of this scheduler (DynamoDB string)
  @VisibleForTesting
  public static final String KEY_SCHEDULER_NAME = "S";

  // The timestamp (and additional random data; please see #buildRunAtAttribute for details) for the job
  // (DynamoDB byte array)
  @VisibleForTesting
  public static final String ATTR_RUN_AT = "T";

  // Additional application-specific data for the job (DynamoDB byte array)
  private static final String ATTR_JOB_DATA = "D";

  // The time at which this job should be garbage-collected if not already deleted (DynamoDB number;
  // seconds from the epoch)
  private static final String ATTR_TTL = "E";

  private static final String SCHEDULE_JOB_COUNTER_NAME = MetricsUtil.name(JobScheduler.class, "scheduleJob");
  private static final String PROCESS_JOB_COUNTER_NAME = MetricsUtil.name(JobScheduler.class, "processJob");

  private static final String SCHEDULER_NAME_TAG = "schedulerName";
  private static final String OUTCOME_TAG = "outcome";

  private static final int MAX_CONCURRENCY = 16;

  protected JobScheduler(final DynamoDbAsyncClient dynamoDbAsyncClient,
      final String tableName,
      final Duration jobExpiration,
      final Clock clock) {

    this.dynamoDbAsyncClient = dynamoDbAsyncClient;
    this.tableName = tableName;
    this.jobExpiration = jobExpiration;
    this.clock = clock;
  }

  /**
   * Returns the unique name of this scheduler. Scheduler names are used to "namespace" jobs.
   *
   * @return the unique name of this scheduler
   */
  public abstract String getSchedulerName();

  /**
   * Processes a previously-scheduled job.
   *
   * @param jobData opaque, application-specific data provided at the time the job was scheduled
   *
   * @return A future that yields a brief, human-readable status code when the job has been fully processed. On
   * successful completion, the job will be deleted. The job will not be deleted if the future completes exceptionally.
   */
  protected abstract CompletableFuture<String> processJob(@Nullable byte[] jobData);

  /**
   * Schedules a job to run at or after the given {@code runAt} time. Concrete implementations must override this method
   * to expose it publicly, or provide some more application-appropriate method for public callers.
   *
   * @param runAt the time at or after which to run the job
   * @param jobData application-specific data describing the job; may be {@code null}
   *
   * @return a future that completes when the job has been scheduled
   */
  protected CompletableFuture<Void> scheduleJob(final Instant runAt, @Nullable final byte[] jobData) {
    return Mono.fromFuture(() -> scheduleJob(buildRunAtAttribute(runAt), runAt.plus(jobExpiration), jobData))
        .retryWhen(Retry.backoff(8, Duration.ofSeconds(1)).maxBackoff(Duration.ofSeconds(4)))
        .toFuture()
        .thenRun(() -> Metrics.counter(SCHEDULE_JOB_COUNTER_NAME, SCHEDULER_NAME_TAG, getSchedulerName()).increment());
  }

  @VisibleForTesting
  CompletableFuture<Void> scheduleJob(final AttributeValue runAt, final Instant expiration, @Nullable final byte[] jobData) {
    final Map<String, AttributeValue> item = new HashMap<>(Map.of(
        KEY_SCHEDULER_NAME, AttributeValue.fromS(getSchedulerName()),
        ATTR_RUN_AT, runAt,
        ATTR_TTL, AttributeValue.fromN(String.valueOf(expiration.getEpochSecond()))));

    if (jobData != null) {
      item.put(ATTR_JOB_DATA, AttributeValue.fromB(SdkBytes.fromByteArray(jobData)));
    }

    return dynamoDbAsyncClient.putItem(PutItemRequest.builder()
            .tableName(tableName)
            .item(item)
            .conditionExpression("attribute_not_exists(#schedulerName)")
            .expressionAttributeNames(Map.of("#schedulerName", KEY_SCHEDULER_NAME))
            .build())
        .thenRun(Util.NOOP);
  }

  /**
   * Finds and processes all jobs whose {@code runAt} time is less than or equal to the current time. Scheduled jobs
   * will be deleted once they have been processed successfully.
   *
   * @return a future that completes when all available jobs have been processed
   *
   * @see #processJob(byte[])
   */
  public Mono<Void> processAvailableJobs() {
    return Flux.from(dynamoDbAsyncClient.queryPaginator(QueryRequest.builder()
            .tableName(tableName)
            .keyConditionExpression("#schedulerName = :schedulerName AND #runAt <= :maxRunAt")
            .expressionAttributeNames(Map.of(
                "#schedulerName", KEY_SCHEDULER_NAME,
                "#runAt", ATTR_RUN_AT))
            .expressionAttributeValues(Map.of(
                ":schedulerName", AttributeValue.fromS(getSchedulerName()),
                ":maxRunAt", buildMaxRunAtAttribute(clock.instant())))
            .build())
        .items())
        .flatMap(item -> {
          final byte[] jobData = item.containsKey(ATTR_JOB_DATA)
              ? item.get(ATTR_JOB_DATA).b().asByteArray()
              : null;

          return Mono.fromFuture(() -> processJob(jobData))
              .doOnNext(outcome -> Metrics.counter(PROCESS_JOB_COUNTER_NAME,
                      SCHEDULER_NAME_TAG, getSchedulerName(),
                      OUTCOME_TAG, outcome)
                  .increment())
              .then(Mono.fromFuture(() -> deleteJob(item.get(KEY_SCHEDULER_NAME), item.get(ATTR_RUN_AT))))
              .onErrorResume(throwable -> {
                logger.warn("Failed to process job", throwable);
                return Mono.empty();
              });
        }, MAX_CONCURRENCY)
        .then();
  }

  private CompletableFuture<Void> deleteJob(final AttributeValue schedulerName, final AttributeValue runAt) {
    return dynamoDbAsyncClient.deleteItem(DeleteItemRequest.builder()
            .tableName(tableName)
            .key(Map.of(
                KEY_SCHEDULER_NAME, schedulerName,
                ATTR_RUN_AT, runAt))
            .build())
        .thenRun(Util.NOOP);
  }

  /**
   * Constructs an attribute value that contains a sort key that will be greater than any sort key generated for an
   * earlier {@code runAt} time and less than a sort key generated for a later {@code runAt} time. The returned value
   * begins with the 8-byte, big-endian representation of the given {@code runAt} time in milliseconds since the epoch
   * and ends with a random 8-byte suffix. The random suffix ensures that multiple jobs scheduled for the same
   * {@code runAt} time will have distinct primary keys; the order in which jobs scheduled at the same time will be
   * executed is also random as a result.
   *
   * @param runAt the time for which to generate a sort key
   *
   * @return a probably-unique sort key for the given {@code runAt} time
   */
  AttributeValue buildRunAtAttribute(final Instant runAt) {
    return buildRunAtAttribute(runAt, ThreadLocalRandom.current().nextLong());
  }

  @VisibleForTesting
  AttributeValue buildRunAtAttribute(final Instant runAt, final long salt) {
    return AttributeValue.fromB(SdkBytes.fromByteBuffer(ByteBuffer.allocate(24)
        .putLong(runAt.toEpochMilli())
        .putLong(clock.millis())
        .putLong(salt)
        .flip()));
  }

  /**
   * Constructs a sort key value that is greater than or equal to all other sort keys for jobs with the same or earlier
   * {@code runAt} time.
   *
   * @param runAt the maximum scheduled time for jobs to match
   *
   * @return an attribute value for a sort key that is greater than or equal to the sort key for all other jobs
   * scheduled to run at or before the given {@code runAt} time
   */
  static AttributeValue buildMaxRunAtAttribute(final Instant runAt) {
    return AttributeValue.fromB(SdkBytes.fromByteBuffer(ByteBuffer.allocate(24)
        .putLong(runAt.toEpochMilli())
        .putLong(0xfffffffffffffffL)
        .putLong(0xfffffffffffffffL)
        .flip()));
  }
}
