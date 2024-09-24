package org.whispersystems.textsecuregcm.scheduler;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtension;
import org.whispersystems.textsecuregcm.storage.DynamoDbExtensionSchema;
import org.whispersystems.textsecuregcm.util.TestClock;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;

class JobSchedulerTest {

  private static final Instant CURRENT_TIME = Instant.now();

  @RegisterExtension
  static final DynamoDbExtension DYNAMO_DB_EXTENSION =
      new DynamoDbExtension(DynamoDbExtensionSchema.Tables.SCHEDULED_JOBS);

  private static class TestJobScheduler extends JobScheduler {

    private final AtomicInteger jobsProcessed = new AtomicInteger(0);

    protected TestJobScheduler(final DynamoDbAsyncClient dynamoDbAsyncClient,
        final String tableName,
        final Clock clock) {

      super(dynamoDbAsyncClient, tableName, Duration.ofDays(7), clock);
    }

    @Override
    public String getSchedulerName() {
      return "test";
    }

    @Override
    protected CompletableFuture<String> processJob(@Nullable final byte[] jobData) {
      jobsProcessed.incrementAndGet();

      return CompletableFuture.completedFuture("test");
    }
  }

  @Test
  void scheduleJob() {
    final TestJobScheduler scheduler = new TestJobScheduler(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.SCHEDULED_JOBS.tableName(),
        Clock.fixed(CURRENT_TIME, ZoneId.systemDefault()));

    assertDoesNotThrow(() ->
        scheduler.scheduleJob(scheduler.buildRunAtAttribute(CURRENT_TIME, 0L), CURRENT_TIME, null).join());

    final CompletionException completionException = assertThrows(CompletionException.class, () ->
        scheduler.scheduleJob(scheduler.buildRunAtAttribute(CURRENT_TIME, 0L), CURRENT_TIME, null).join(),
        "Scheduling multiple jobs with identical sort keys should fail");

    assertInstanceOf(ConditionalCheckFailedException.class, completionException.getCause());
  }

  @Test
  void processAvailableJobs() {
    final TestClock testClock = TestClock.pinned(CURRENT_TIME);

    final TestJobScheduler scheduler = new TestJobScheduler(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.SCHEDULED_JOBS.tableName(),
        testClock);

    scheduler.scheduleJob(scheduler.buildRunAtAttribute(CURRENT_TIME, 0L), CURRENT_TIME, null).join();

    // Clock time is before scheduled job time
    testClock.pin(CURRENT_TIME.minusMillis(1));

    scheduler.processAvailableJobs().block();
    assertEquals(0, scheduler.jobsProcessed.get());

    // Clock time is after scheduled job time
    testClock.pin(CURRENT_TIME.plusMillis(1));

    scheduler.processAvailableJobs().block();
    assertEquals(1, scheduler.jobsProcessed.get());

    scheduler.processAvailableJobs().block();
    assertEquals(1, scheduler.jobsProcessed.get(),
        "Jobs should be cleared after successful processing; job counter should not increment on second run");
  }

  @Test
  void processAvailableJobsWithError() {
    final AtomicInteger jobsEncountered = new AtomicInteger(0);

    final TestJobScheduler scheduler = new TestJobScheduler(DYNAMO_DB_EXTENSION.getDynamoDbAsyncClient(),
        DynamoDbExtensionSchema.Tables.SCHEDULED_JOBS.tableName(),
        Clock.fixed(CURRENT_TIME, ZoneId.systemDefault())) {

      @Override
      protected CompletableFuture<String> processJob(@Nullable final byte[] jobData) {
        jobsEncountered.incrementAndGet();

        return CompletableFuture.failedFuture(new RuntimeException("OH NO"));
      }
    };

    scheduler.scheduleJob(scheduler.buildRunAtAttribute(CURRENT_TIME, 0L), CURRENT_TIME, null).join();

    scheduler.processAvailableJobs().block();
    assertEquals(1, jobsEncountered.get());

    scheduler.processAvailableJobs().block();
    assertEquals(2, jobsEncountered.get(),
        "Jobs should not be cleared after failed processing; encountered job counter should increment on second run");
  }
}
