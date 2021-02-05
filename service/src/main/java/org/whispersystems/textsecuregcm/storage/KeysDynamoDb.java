/*
 * Copyright 2021 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.storage;

import com.amazonaws.services.dynamodbv2.document.DeleteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.Select;
import com.google.common.annotations.VisibleForTesting;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;
import org.whispersystems.textsecuregcm.entities.PreKey;
import org.whispersystems.textsecuregcm.util.UUIDUtil;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

public class KeysDynamoDb extends AbstractDynamoDbStore {

    private final Table table;

    static final String KEY_ACCOUNT_UUID = "U";
    static final String KEY_DEVICE_ID_KEY_ID = "DK";
    static final String KEY_PUBLIC_KEY = "P";

    private static final Timer               STORE_KEYS_TIMER              = Metrics.timer(name(KeysDynamoDb.class, "storeKeys"));
    private static final Timer               TAKE_KEY_FOR_DEVICE_TIMER     = Metrics.timer(name(KeysDynamoDb.class, "takeKeyForDevice"));
    private static final Timer               TAKE_KEYS_FOR_ACCOUNT_TIMER   = Metrics.timer(name(KeysDynamoDb.class, "takeKeyForAccount"));
    private static final Timer               GET_KEY_COUNT_TIMER           = Metrics.timer(name(KeysDynamoDb.class, "getKeyCount"));
    private static final Timer               DELETE_KEYS_FOR_DEVICE_TIMER  = Metrics.timer(name(KeysDynamoDb.class, "deleteKeysForDevice"));
    private static final Timer               DELETE_KEYS_FOR_ACCOUNT_TIMER = Metrics.timer(name(KeysDynamoDb.class, "deleteKeysForAccount"));
    private static final DistributionSummary CONTESTED_KEY_DISTRIBUTION    = Metrics.summary(name(KeysDynamoDb.class, "contestedKeys"));

    public KeysDynamoDb(final DynamoDB dynamoDB, final String tableName) {
        super(dynamoDB);

        this.table = dynamoDB.getTable(tableName);
    }

    public void store(final Account account, final long deviceId, final List<PreKey> keys) {
        STORE_KEYS_TIMER.record(() -> {
            delete(account, deviceId);

            writeInBatches(keys, batch -> {
                final TableWriteItems items = new TableWriteItems(table.getTableName());

                for (final PreKey preKey : batch) {
                    items.addItemToPut(getItemFromPreKey(account.getUuid(), deviceId, preKey));
                }

                executeTableWriteItemsUntilComplete(items);
            });
        });
    }

    public List<KeyRecord> take(final Account account, final long deviceId) {
        return TAKE_KEY_FOR_DEVICE_TIMER.record(() -> {
            final byte[] partitionKey = getPartitionKey(account.getUuid());

            final QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("#uuid = :uuid AND begins_with (#sort, :sortprefix)")
                                                       .withNameMap(Map.of("#uuid", KEY_ACCOUNT_UUID, "#sort", KEY_DEVICE_ID_KEY_ID))
                                                       .withValueMap(Map.of(":uuid", partitionKey,
                                                                            ":sortprefix", getSortKeyPrefix(deviceId)))
                                                       .withProjectionExpression(KEY_DEVICE_ID_KEY_ID)
                                                       .withConsistentRead(false);

            int contestedKeys = 0;

            try {
                for (final Item candidate : table.query(querySpec)) {
                    final DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey(KEY_ACCOUNT_UUID, partitionKey, KEY_DEVICE_ID_KEY_ID, candidate.getBinary(KEY_DEVICE_ID_KEY_ID))
                                                                              .withReturnValues(ReturnValue.ALL_OLD);

                    final DeleteItemOutcome outcome = table.deleteItem(deleteItemSpec);

                    if (outcome.getItem() != null) {
                        final PreKey preKey = getPreKeyFromItem(outcome.getItem());
                        return List.of(new KeyRecord(-1, account.getNumber(), deviceId, preKey.getKeyId(), preKey.getPublicKey()));
                    }

                    contestedKeys++;
                }

                return Collections.emptyList();
            } finally {
                CONTESTED_KEY_DISTRIBUTION.record(contestedKeys);
            }
        });
    }

    public List<KeyRecord> take(final Account account) {
        return TAKE_KEYS_FOR_ACCOUNT_TIMER.record(() -> {
            final List<KeyRecord> keyRecords = new ArrayList<>();

            for (final Device device : account.getDevices()) {
                keyRecords.addAll(take(account, device.getId()));
            }

            return keyRecords;
        });
    }

    public int getCount(final Account account, final long deviceId) {
        return GET_KEY_COUNT_TIMER.record(() -> {
            final QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("#uuid = :uuid AND begins_with (#sort, :sortprefix)")
                                                       .withNameMap(Map.of("#uuid", KEY_ACCOUNT_UUID, "#sort", KEY_DEVICE_ID_KEY_ID))
                                                       .withValueMap(Map.of(":uuid", getPartitionKey(account.getUuid()),
                                                                            ":sortprefix", getSortKeyPrefix(deviceId)))
                                                       .withSelect(Select.COUNT)
                                                       .withConsistentRead(false);

            return (int)countItemsMatchingQuery(table, querySpec);
        });
    }

    public void delete(final Account account) {
        DELETE_KEYS_FOR_ACCOUNT_TIMER.record(() -> {
            final QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("#uuid = :uuid")
                                                       .withNameMap(Map.of("#uuid", KEY_ACCOUNT_UUID))
                                                       .withValueMap(Map.of(":uuid", getPartitionKey(account.getUuid())))
                                                       .withProjectionExpression(KEY_DEVICE_ID_KEY_ID)
                                                       .withConsistentRead(true);

            deleteItemsForAccountMatchingQuery(account, querySpec);
        });
    }

    @VisibleForTesting
    void delete(final Account account, final long deviceId) {
        DELETE_KEYS_FOR_DEVICE_TIMER.record(() -> {
            final QuerySpec querySpec = new QuerySpec().withKeyConditionExpression("#uuid = :uuid AND begins_with (#sort, :sortprefix)")
                                                       .withNameMap(Map.of("#uuid", KEY_ACCOUNT_UUID, "#sort", KEY_DEVICE_ID_KEY_ID))
                                                       .withValueMap(Map.of(":uuid", getPartitionKey(account.getUuid()),
                                                                            ":sortprefix", getSortKeyPrefix(deviceId)))
                                                       .withProjectionExpression(KEY_DEVICE_ID_KEY_ID)
                                                       .withConsistentRead(true);

            deleteItemsForAccountMatchingQuery(account, querySpec);
        });
    }

    private void deleteItemsForAccountMatchingQuery(final Account account, final QuerySpec querySpec) {
        final byte[] partitionKey = getPartitionKey(account.getUuid());

        writeInBatches(table.query(querySpec), batch -> {
            final TableWriteItems writeItems = new TableWriteItems(table.getTableName());

            for (final Item item : batch) {
                writeItems.addPrimaryKeyToDelete(new PrimaryKey(KEY_ACCOUNT_UUID, partitionKey, KEY_DEVICE_ID_KEY_ID, item.getBinary(KEY_DEVICE_ID_KEY_ID)));
            }

            executeTableWriteItemsUntilComplete(writeItems);
        });
    }

    private static byte[] getPartitionKey(final UUID accountUuid) {
        return UUIDUtil.toBytes(accountUuid);
    }

    private static byte[] getSortKey(final long deviceId, final long keyId) {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[16]);
        byteBuffer.putLong(deviceId);
        byteBuffer.putLong(keyId);
        return byteBuffer.array();
    }

    private static byte[] getSortKeyPrefix(final long deviceId) {
        final ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[8]);
        byteBuffer.putLong(deviceId);
        return byteBuffer.array();
    }

    private Item getItemFromPreKey(final UUID accountUuid, final long deviceId, final PreKey preKey) {
        return new Item().withBinary(KEY_ACCOUNT_UUID, getPartitionKey(accountUuid))
                         .withBinary(KEY_DEVICE_ID_KEY_ID, getSortKey(deviceId, preKey.getKeyId()))
                         .withString(KEY_PUBLIC_KEY, preKey.getPublicKey());
    }

    private PreKey getPreKeyFromItem(final Item item) {
        final long keyId = ByteBuffer.wrap(item.getBinary(KEY_DEVICE_ID_KEY_ID)).getLong(8);
        return new PreKey(keyId, item.getString(KEY_PUBLIC_KEY));
    }
}
