/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import org.junit.Test;
import org.whispersystems.textsecuregcm.tests.util.RedisClusterHelper;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ClusterLuaScriptTest extends AbstractRedisClusterTest {

    @Test
    public void testExecuteMovedKey() {
        final String key   = "key";
        final String value = "value";

        final FaultTolerantRedisCluster redisCluster = getRedisCluster();

        final ClusterLuaScript script = new ClusterLuaScript(redisCluster, "return redis.call(\"SET\", KEYS[1], ARGV[1])", ScriptOutputType.VALUE);

        assertEquals("OK", script.execute(List.of(key), List.of(value)));
        assertEquals(value, redisCluster.withCluster(connection -> connection.sync().get(key)));

        final int    slot            = SlotHash.getSlot(key);

        final int                           sourcePort          = redisCluster.withCluster(connection -> connection.sync().nodes(node -> node.hasSlot(slot) && node.is(RedisClusterNode.NodeFlag.MASTER)).node(0).getUri().getPort());
        final RedisCommands<String, String> sourceCommands      = redisCluster.withCluster(connection -> connection.sync().nodes(node -> node.hasSlot(slot) && node.is(RedisClusterNode.NodeFlag.MASTER)).commands(0));
        final RedisCommands<String, String> destinationCommands = redisCluster.withCluster(connection -> connection.sync().nodes(node -> !node.hasSlot(slot) && node.is(RedisClusterNode.NodeFlag.MASTER)).commands(0));

        destinationCommands.clusterSetSlotImporting(slot, sourceCommands.clusterMyId());

        assertEquals("OK", script.execute(List.of(key), List.of(value)));
        assertEquals(value, redisCluster.withCluster(connection -> connection.sync().get(key)));

        sourceCommands.clusterSetSlotMigrating(slot, destinationCommands.clusterMyId());

        assertEquals("OK", script.execute(List.of(key), List.of(value)));
        assertEquals(value, redisCluster.withCluster(connection -> connection.sync().get(key)));

        for (final String migrateKey : sourceCommands.clusterGetKeysInSlot(slot, Integer.MAX_VALUE)) {
            destinationCommands.migrate("127.0.0.1", sourcePort, migrateKey, 0, 1000);
        }

        assertEquals("OK", script.execute(List.of(key), List.of(value)));
        assertEquals(value, redisCluster.withCluster(connection -> connection.sync().get(key)));

        destinationCommands.clusterSetSlotNode(slot, destinationCommands.clusterMyId());

        assertEquals("OK", script.execute(List.of(key), List.of(value)));
        assertEquals(value, redisCluster.withCluster(connection -> connection.sync().get(key)));
    }

    @Test
    public void testExecute() {
        final RedisAdvancedClusterCommands<String, String> commands    = mock(RedisAdvancedClusterCommands.class);
        final FaultTolerantRedisCluster                    mockCluster = RedisClusterHelper.buildMockRedisCluster(commands);

        final String           script           = "return redis.call(\"SET\", KEYS[1], ARGV[1])";
        final String           sha              = "abc123";
        final ScriptOutputType scriptOutputType = ScriptOutputType.VALUE;
        final List<String>     keys             = List.of("key");
        final List<String>     values           = List.of("value");

        when(commands.scriptLoad(script)).thenReturn(sha);
        when(commands.evalsha(any(), any(), any(), any())).thenReturn("OK");

        new ClusterLuaScript(mockCluster, script, scriptOutputType).execute(keys, values);

        verify(commands).scriptLoad(script);
        verify(commands).evalsha(sha, scriptOutputType, keys.toArray(new String[0]), values.toArray(new String[0]));
    }

    @Test
    public void testExecuteNoScriptException() {
        final String key   = "key";
        final String value = "value";

        final FaultTolerantRedisCluster redisCluster = getRedisCluster();

        final ClusterLuaScript script = new ClusterLuaScript(redisCluster, "return redis.call(\"SET\", KEYS[1], ARGV[1])", ScriptOutputType.VALUE);

        // Remove the scripts created by the CLusterLuaScript constructor
        redisCluster.useCluster(connection -> connection.sync().upstream().commands().scriptFlush());

        assertEquals("OK", script.execute(List.of(key), List.of(value)));
        assertEquals(value, redisCluster.withCluster(connection -> connection.sync().get(key)));
    }

    @Test
    public void testExecuteBinary() {
        final RedisAdvancedClusterCommands<String, String> stringCommands = mock(RedisAdvancedClusterCommands.class);
        final RedisAdvancedClusterCommands<byte[], byte[]> binaryCommands = mock(RedisAdvancedClusterCommands.class);
        final FaultTolerantRedisCluster                    mockCluster    = RedisClusterHelper.buildMockRedisCluster(stringCommands, binaryCommands);

        final String           script           = "return redis.call(\"SET\", KEYS[1], ARGV[1])";
        final String           sha              = "abc123";
        final ScriptOutputType scriptOutputType = ScriptOutputType.VALUE;
        final List<byte[]>     keys             = List.of("key".getBytes(StandardCharsets.UTF_8));
        final List<byte[]>     values           = List.of("value".getBytes(StandardCharsets.UTF_8));

        when(stringCommands.scriptLoad(script)).thenReturn(sha);
        when(binaryCommands.evalsha(any(), any(), any(), any())).thenReturn("OK".getBytes(StandardCharsets.UTF_8));

        new ClusterLuaScript(mockCluster, script, scriptOutputType).executeBinary(keys, values);

        verify(stringCommands).scriptLoad(script);
        verify(binaryCommands).evalsha(sha, scriptOutputType, keys.toArray(new byte[0][]), values.toArray(new byte[0][]));
    }

    @Test
    public void testExecuteBinaryNoScriptException() {
        final String key   = "key";
        final String value = "value";

        final FaultTolerantRedisCluster redisCluster = getRedisCluster();

        final ClusterLuaScript script = new ClusterLuaScript(redisCluster, "return redis.call(\"SET\", KEYS[1], ARGV[1])", ScriptOutputType.VALUE);

        // Remove the scripts created by the CLusterLuaScript constructor
        redisCluster.useCluster(connection -> connection.sync().upstream().commands().scriptFlush());

        assertArrayEquals("OK".getBytes(StandardCharsets.UTF_8), (byte[])script.executeBinary(List.of(key.getBytes(StandardCharsets.UTF_8)), List.of(value.getBytes(StandardCharsets.UTF_8))));
        assertEquals(value, redisCluster.withCluster(connection -> connection.sync().get(key)));
    }
}
