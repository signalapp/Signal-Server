package org.whispersystems.textsecuregcm.storage;

import com.google.protobuf.InvalidProtocolBufferException;
import io.lettuce.core.ScriptOutputType;
import io.micrometer.core.instrument.Metrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.MessageProtos;
import org.whispersystems.textsecuregcm.entities.OutgoingMessageEntity;
import org.whispersystems.textsecuregcm.redis.ClusterLuaScript;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.RedisClusterUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

public class RedisClusterMessagesCache implements UserMessagesCache {

    private final ClusterLuaScript insertScript;
    private final ClusterLuaScript removeByIdScript;
    private final ClusterLuaScript removeBySenderScript;
    private final ClusterLuaScript removeByGuidScript;
    private final ClusterLuaScript getItemsScript;
    private final ClusterLuaScript removeQueueScript;

    private static final String INSERT_TIMER_NAME = name(RedisClusterMessagesCache.class, "insert");
    private static final String REMOVE_TIMER_NAME = name(RedisClusterMessagesCache.class, "remove");
    private static final String GET_TIMER_NAME    = name(RedisClusterMessagesCache.class, "get");
    private static final String CLEAR_TIMER_NAME  = name(RedisClusterMessagesCache.class, "clear");

    private static final String REMOVE_METHOD_TAG    = "method";
    private static final String REMOVE_METHOD_ID     = "id";
    private static final String REMOVE_METHOD_SENDER = "sender";
    private static final String REMOVE_METHOD_UUID   = "uuid";

    private static final Logger logger = LoggerFactory.getLogger(RedisClusterMessagesCache.class);

    public RedisClusterMessagesCache(final FaultTolerantRedisCluster redisCluster) throws IOException {

        this.insertScript         = ClusterLuaScript.fromResource(redisCluster, "lua/insert_item.lua",           ScriptOutputType.INTEGER);
        this.removeByIdScript     = ClusterLuaScript.fromResource(redisCluster, "lua/remove_item_by_id.lua",     ScriptOutputType.VALUE);
        this.removeBySenderScript = ClusterLuaScript.fromResource(redisCluster, "lua/remove_item_by_sender.lua", ScriptOutputType.VALUE);
        this.removeByGuidScript   = ClusterLuaScript.fromResource(redisCluster, "lua/remove_item_by_guid.lua",   ScriptOutputType.VALUE);
        this.getItemsScript       = ClusterLuaScript.fromResource(redisCluster, "lua/get_items.lua",             ScriptOutputType.MULTI);
        this.removeQueueScript    = ClusterLuaScript.fromResource(redisCluster, "lua/remove_queue.lua",          ScriptOutputType.STATUS);
    }

    @Override
    public long insert(final UUID guid, final String destination, final long destinationDevice, final MessageProtos.Envelope message) {
        final MessageProtos.Envelope messageWithGuid = message.toBuilder().setServerGuid(guid.toString()).build();
        final String                 sender          = message.hasSource() ? (message.getSource() + "::" + message.getTimestamp()) : "nil";

        return (long)Metrics.timer(INSERT_TIMER_NAME).record(() ->
                insertScript.executeBinary(List.of(getMessageQueueKey(destination, destinationDevice),
                                                   getMessageQueueMetadataKey(destination, destinationDevice),
                                                   getQueueIndexKey(destination, destinationDevice)),
                                           List.of(messageWithGuid.toByteArray(),
                                                   String.valueOf(message.getTimestamp()).getBytes(StandardCharsets.UTF_8),
                                                   sender.getBytes(StandardCharsets.UTF_8),
                                                   guid.toString().getBytes(StandardCharsets.UTF_8))));
    }

    public long insert(final UUID guid, final String destination, final long destinationDevice, final MessageProtos.Envelope message, final long messageId) {
        final MessageProtos.Envelope messageWithGuid = message.toBuilder().setServerGuid(guid.toString()).build();
        final String                 sender          = message.hasSource() ? (message.getSource() + "::" + message.getTimestamp()) : "nil";

        return (long)Metrics.timer(INSERT_TIMER_NAME).record(() ->
                insertScript.executeBinary(List.of(getMessageQueueKey(destination, destinationDevice),
                                                   getMessageQueueMetadataKey(destination, destinationDevice),
                                                   getQueueIndexKey(destination, destinationDevice)),
                                           List.of(messageWithGuid.toByteArray(),
                                                   String.valueOf(message.getTimestamp()).getBytes(StandardCharsets.UTF_8),
                                                   sender.getBytes(StandardCharsets.UTF_8),
                                                   guid.toString().getBytes(StandardCharsets.UTF_8),
                                                   String.valueOf(messageId).getBytes(StandardCharsets.UTF_8))));
    }

    @Override
    public Optional<OutgoingMessageEntity> remove(final String destination, final long destinationDevice, final long id) {
        try {
            final byte[] serialized = (byte[])Metrics.timer(REMOVE_TIMER_NAME, REMOVE_METHOD_TAG, REMOVE_METHOD_ID).record(() ->
                    removeByIdScript.executeBinary(List.of(getMessageQueueKey(destination, destinationDevice),
                                                           getMessageQueueMetadataKey(destination, destinationDevice),
                                                           getQueueIndexKey(destination, destinationDevice)),
                                                   List.of(String.valueOf(id).getBytes(StandardCharsets.UTF_8))));


            if (serialized != null) {
                return Optional.of(UserMessagesCache.constructEntityFromEnvelope(id, MessageProtos.Envelope.parseFrom(serialized)));
            }
        } catch (final InvalidProtocolBufferException e) {
            logger.warn("Failed to parse envelope", e);
        }

        return Optional.empty();
    }

    @Override
    public Optional<OutgoingMessageEntity> remove(final String destination, final long destinationDevice, final String sender, final long timestamp) {
        try {
            final byte[] serialized = (byte[])Metrics.timer(REMOVE_TIMER_NAME, REMOVE_METHOD_TAG, REMOVE_METHOD_SENDER).record(() ->
                    removeBySenderScript.executeBinary(List.of(getMessageQueueKey(destination, destinationDevice),
                                                               getMessageQueueMetadataKey(destination, destinationDevice),
                                                               getQueueIndexKey(destination, destinationDevice)),
                                                       List.of((sender + "::" + timestamp).getBytes(StandardCharsets.UTF_8))));

            if (serialized != null) {
                return Optional.of(UserMessagesCache.constructEntityFromEnvelope(0, MessageProtos.Envelope.parseFrom(serialized)));
            }
        } catch (final InvalidProtocolBufferException e) {
            logger.warn("Failed to parse envelope", e);
        }

        return Optional.empty();
    }

    @Override
    public Optional<OutgoingMessageEntity> remove(final String destination, final long destinationDevice, final UUID guid) {
        try {
            final byte[] serialized = (byte[])Metrics.timer(REMOVE_TIMER_NAME, REMOVE_METHOD_TAG, REMOVE_METHOD_UUID).record(() ->
                    removeByGuidScript.executeBinary(List.of(getMessageQueueKey(destination, destinationDevice),
                                                             getMessageQueueMetadataKey(destination, destinationDevice),
                                                             getQueueIndexKey(destination, destinationDevice)),
                                                     List.of(guid.toString().getBytes(StandardCharsets.UTF_8))));

            if (serialized != null) {
                return Optional.of(UserMessagesCache.constructEntityFromEnvelope(0, MessageProtos.Envelope.parseFrom(serialized)));
            }
        } catch (final InvalidProtocolBufferException e) {
            logger.warn("Failed to parse envelope", e);
        }

        return Optional.empty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<OutgoingMessageEntity> get(String destination, long destinationDevice, int limit) {
        return Metrics.timer(GET_TIMER_NAME).record(() -> {
            final List<byte[]> queueItems = (List<byte[]>)getItemsScript.executeBinary(List.of(getMessageQueueKey(destination, destinationDevice),
                                                                                               getPersistInProgressKey(destination, destinationDevice)),
                                                                                       List.of(String.valueOf(limit).getBytes()));

            final List<OutgoingMessageEntity> messageEntities;

            if (queueItems.size() % 2 == 0) {
                messageEntities = new ArrayList<>(queueItems.size() / 2);

                for (int i = 0; i < queueItems.size() - 1; i += 2) {
                    try {
                        final MessageProtos.Envelope message = MessageProtos.Envelope.parseFrom(queueItems.get(i));
                        final long id = Long.parseLong(new String(queueItems.get(i + 1), StandardCharsets.UTF_8));

                        messageEntities.add(UserMessagesCache.constructEntityFromEnvelope(id, message));
                    } catch (InvalidProtocolBufferException e) {
                        logger.warn("Failed to parse envelope", e);
                    }
                }
            } else {
                logger.error("\"Get messages\" operation returned a list with a non-even number of elements.");
                messageEntities = Collections.emptyList();
            }

            return messageEntities;
        });
    }

    @Override
    public void clear(final String destination) {
        for (int i = 1; i < 256; i++) {
            clear(destination, i);
        }
    }

    @Override
    public void clear(final String destination, final long deviceId) {
        Metrics.timer(CLEAR_TIMER_NAME).record(() ->
                removeQueueScript.executeBinary(List.of(getMessageQueueKey(destination, deviceId),
                                                        getMessageQueueMetadataKey(destination, deviceId),
                                                        getQueueIndexKey(destination, deviceId)),
                                                Collections.emptyList()));
    }

    private static byte[] getMessageQueueKey(final String address, final long deviceId) {
        return ("user_queue::{" + address + "::" + deviceId + "}").getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] getMessageQueueMetadataKey(final String address, final long deviceId) {
        return ("user_queue_metadata::{" + address + "::" + deviceId + "}").getBytes(StandardCharsets.UTF_8);
    }

    private byte[] getQueueIndexKey(final String address, final long deviceId) {
        return ("user_queue_index::{" + RedisClusterUtil.getMinimalHashTag(address + "::" + deviceId) + "}").getBytes(StandardCharsets.UTF_8);
    }

    private byte[] getPersistInProgressKey(final String address, final long deviceId) {
        return ("user_queue_persisting::{" + address + "::" + deviceId + "}").getBytes(StandardCharsets.UTF_8);
    }
}
