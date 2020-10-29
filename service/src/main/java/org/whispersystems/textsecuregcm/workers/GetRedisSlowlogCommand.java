/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import io.lettuce.core.resource.ClientResources;
import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.SystemMapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetRedisSlowlogCommand extends ConfiguredCommand<WhisperServerConfiguration> {

    public GetRedisSlowlogCommand() {
        super("redisslowlog", "Dump a JSON blob describing slow Redis operations");
    }

    @Override
    public void configure(final Subparser subparser) {
        super.configure(subparser);

        subparser.addArgument("-n", "--entries")
                 .dest("entries")
                 .type(Integer.class)
                 .required(false)
                 .setDefault(128)
                 .help("The maximum number of SLOWLOG entries to retrieve per cluster node");
    }

    @Override
    protected void run(final Bootstrap<WhisperServerConfiguration> bootstrap, final Namespace namespace, final WhisperServerConfiguration config) throws Exception {
        final int entries = namespace.getInt("entries");

        final ClientResources redisClusterClientResources = ClientResources.builder().build();

        final FaultTolerantRedisCluster cacheCluster         = new FaultTolerantRedisCluster("main_cache_cluster", config.getCacheClusterConfiguration(), redisClusterClientResources);
        final FaultTolerantRedisCluster messagesCacheCluster = new FaultTolerantRedisCluster("messages_cluster", config.getMessageCacheConfiguration().getRedisClusterConfiguration(), redisClusterClientResources);
        final FaultTolerantRedisCluster metricsCluster       = new FaultTolerantRedisCluster("metrics_cluster", config.getMetricsClusterConfiguration(), redisClusterClientResources);

        final Map<String, List<Object>> slowlogsByUri = new HashMap<>();

        for (final FaultTolerantRedisCluster cluster : List.of(cacheCluster, messagesCacheCluster, metricsCluster)) {
            cluster.useCluster(connection -> connection.sync()
                                                       .masters()
                                                       .commands()
                                                       .slowlogGet(entries)
                                                       .asMap()
                                                       .forEach((node, slowlogs) -> slowlogsByUri.put(node.getUri().toString(), slowlogs)));
        }

        SystemMapper.getMapper().writeValue(System.out, slowlogsByUri);
    }
}
