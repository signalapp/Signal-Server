/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.redis;

import com.google.common.annotations.VisibleForTesting;
import io.lettuce.core.RedisNoScriptException;
import io.lettuce.core.ScriptOutputType;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.cluster.SlotHash;
import io.lettuce.core.cluster.api.sync.RedisAdvancedClusterCommands;
import io.lettuce.core.cluster.models.partitions.RedisClusterNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class ClusterLuaScript {

    private final FaultTolerantRedisCluster redisCluster;
    private final ScriptOutputType          scriptOutputType;
    private final String                    script;
    private final String                    sha;

    private static final String[] STRING_ARRAY     = new String[0];
    private static final byte[][] BYTE_ARRAY_ARRAY = new byte[0][];

    private static final Logger log = LoggerFactory.getLogger(ClusterLuaScript.class);

    public static ClusterLuaScript fromResource(final FaultTolerantRedisCluster redisCluster, final String resource, final ScriptOutputType scriptOutputType) throws IOException {
        try (final InputStream inputStream    = LuaScript.class.getClassLoader().getResourceAsStream(resource);
             final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {

            byte[] buffer = new byte[4096];
            int read;

            while ((read = inputStream.read(buffer)) != -1) {
                baos.write(buffer, 0, read);
            }

            return new ClusterLuaScript(redisCluster, new String(baos.toByteArray()), scriptOutputType);
        }
    }

    @VisibleForTesting
    ClusterLuaScript(final FaultTolerantRedisCluster redisCluster, final String script, final ScriptOutputType scriptOutputType) {
        this.redisCluster     = redisCluster;
        this.scriptOutputType = scriptOutputType;
        this.script           = script;
        this.sha              = redisCluster.withCluster(connection -> connection.sync().scriptLoad(script));
    }

    public Object execute(final List<String> keys, final List<String> args) {
        return redisCluster.withCluster(connection -> {
            try {
                final RedisAdvancedClusterCommands<String, String> clusterCommands = connection.sync();

                try {
                    return clusterCommands.evalsha(sha, scriptOutputType, keys.toArray(STRING_ARRAY), args.toArray(STRING_ARRAY));
                } catch (final RedisNoScriptException e) {
                    reloadScript();
                    return clusterCommands.evalsha(sha, scriptOutputType, keys.toArray(STRING_ARRAY), args.toArray(STRING_ARRAY));
                }
            } catch (final Exception e) {
                log.warn("Failed to execute script", e);
                throw e;
            }
        });
    }

    public Object executeBinary(final List<byte[]> keys, final List<byte[]> args) {
        return redisCluster.withBinaryCluster(connection -> {
            try {
                final RedisAdvancedClusterCommands<byte[], byte[]> binaryCommands = connection.sync();

                try {
                    return binaryCommands.evalsha(sha, scriptOutputType, keys.toArray(BYTE_ARRAY_ARRAY), args.toArray(BYTE_ARRAY_ARRAY));
                } catch (final RedisNoScriptException e) {
                    reloadScript();
                    return binaryCommands.evalsha(sha, scriptOutputType, keys.toArray(BYTE_ARRAY_ARRAY), args.toArray(BYTE_ARRAY_ARRAY));
                }
            } catch (final Exception e) {
                log.warn("Failed to execute script", e);
                throw e;
            }
        });
    }

    private void reloadScript() {
        redisCluster.useCluster(connection -> connection.sync().upstream().commands().scriptLoad(script));
    }
}
