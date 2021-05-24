/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.codahale.metrics.MetricRegistry.name;
import static io.micrometer.core.instrument.Metrics.counter;
import static io.micrometer.core.instrument.Metrics.timer;

public class AbstractDynamoDbStore {

    private final DynamoDbClient dynamoDbClient;

    private final Timer   batchWriteItemsFirstPass   = timer(name(getClass(), "batchWriteItems"), "firstAttempt", "true");
    private final Timer   batchWriteItemsRetryPass   = timer(name(getClass(), "batchWriteItems"), "firstAttempt", "false");
    private final Counter batchWriteItemsUnprocessed = counter(name(getClass(), "batchWriteItemsUnprocessed"));

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final int MAX_ATTEMPTS_TO_SAVE_BATCH_WRITE = 25;  // This was arbitrarily chosen and may be entirely too high.
    public static final int DYNAMO_DB_MAX_BATCH_SIZE = 25;  // This limit comes from Amazon Dynamo DB itself. It will reject batch writes larger than this.
    public static final int RESULT_SET_CHUNK_SIZE = 100;

    public AbstractDynamoDbStore(final DynamoDbClient dynamoDbClient) {
        this.dynamoDbClient = dynamoDbClient;
    }

    protected DynamoDbClient db() {
        return dynamoDbClient;
    }

    protected void executeTableWriteItemsUntilComplete(final Map<String,List<WriteRequest>> items) {
        AtomicReference<BatchWriteItemResponse> outcome = new AtomicReference<>();
        batchWriteItemsFirstPass.record(() -> outcome.set(dynamoDbClient.batchWriteItem(BatchWriteItemRequest.builder().requestItems(items).build())));
        int attemptCount = 0;
        while (!outcome.get().unprocessedItems().isEmpty() && attemptCount < MAX_ATTEMPTS_TO_SAVE_BATCH_WRITE) {
            batchWriteItemsRetryPass.record(() -> outcome.set(dynamoDbClient.batchWriteItem(BatchWriteItemRequest.builder()
                .requestItems(outcome.get().unprocessedItems())
                .build())));
            ++attemptCount;
        }
        if (!outcome.get().unprocessedItems().isEmpty()) {
          int totalItems = outcome.get().unprocessedItems().values().stream().mapToInt(List::size).sum();
            logger.error("Attempt count ({}) reached max ({}}) before applying all batch writes to dynamo. {} unprocessed items remain.", attemptCount, MAX_ATTEMPTS_TO_SAVE_BATCH_WRITE, totalItems);
            batchWriteItemsUnprocessed.increment(totalItems);
        }
    }

    static <T> void writeInBatches(final Iterable<T> items, final Consumer<List<T>> action) {
        final List<T> batch = new ArrayList<>(DYNAMO_DB_MAX_BATCH_SIZE);

        for (T item : items) {
            batch.add(item);

            if (batch.size() == DYNAMO_DB_MAX_BATCH_SIZE) {
                action.accept(batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            action.accept(batch);
        }
    }
}
