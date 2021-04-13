/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Page;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.codahale.metrics.MetricRegistry.name;
import static io.micrometer.core.instrument.Metrics.counter;
import static io.micrometer.core.instrument.Metrics.timer;

public class AbstractDynamoDbStore {

    private final DynamoDB dynamoDb;

    private final Timer   batchWriteItemsFirstPass   = timer(name(getClass(), "batchWriteItems"), "firstAttempt", "true");
    private final Timer   batchWriteItemsRetryPass   = timer(name(getClass(), "batchWriteItems"), "firstAttempt", "false");
    private final Counter batchWriteItemsUnprocessed = counter(name(getClass(), "batchWriteItemsUnprocessed"));

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static final int MAX_ATTEMPTS_TO_SAVE_BATCH_WRITE = 25;  // This was arbitrarily chosen and may be entirely too high.
    public static final int DYNAMO_DB_MAX_BATCH_SIZE = 25;  // This limit comes from Amazon Dynamo DB itself. It will reject batch writes larger than this.
    public static final int RESULT_SET_CHUNK_SIZE = 100;

    public AbstractDynamoDbStore(final DynamoDB dynamoDb) {
        this.dynamoDb = dynamoDb;
    }

    protected DynamoDB getDynamoDb() {
        return dynamoDb;
    }

    protected void executeTableWriteItemsUntilComplete(final TableWriteItems items) {
        AtomicReference<BatchWriteItemOutcome> outcome = new AtomicReference<>();
        batchWriteItemsFirstPass.record(() -> outcome.set(dynamoDb.batchWriteItem(items)));
        int attemptCount = 0;
        while (!outcome.get().getUnprocessedItems().isEmpty() && attemptCount < MAX_ATTEMPTS_TO_SAVE_BATCH_WRITE) {
            batchWriteItemsRetryPass.record(() -> outcome.set(dynamoDb.batchWriteItemUnprocessed(outcome.get().getUnprocessedItems())));
            ++attemptCount;
        }
        if (!outcome.get().getUnprocessedItems().isEmpty()) {
            logger.error("Attempt count ({}) reached max ({}}) before applying all batch writes to dynamo. {} unprocessed items remain.", attemptCount, MAX_ATTEMPTS_TO_SAVE_BATCH_WRITE, outcome.get().getUnprocessedItems().size());
            batchWriteItemsUnprocessed.increment(outcome.get().getUnprocessedItems().size());
        }
    }

    protected long countItemsMatchingQuery(final Table table, final QuerySpec querySpec) {
        // This is very confusing, but does appear to be the intended behavior. See:
        //
        // - https://github.com/aws/aws-sdk-java/issues/693
        // - https://github.com/aws/aws-sdk-java/issues/915
        // - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.Count

        long matchingItems = 0;

        for (final Page<Item, QueryOutcome> page : table.query(querySpec).pages()) {
            matchingItems += page.getLowLevelResult().getQueryResult().getCount();
        }

        return matchingItems;
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
