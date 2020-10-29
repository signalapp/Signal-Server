/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import io.lettuce.core.resource.ClientResources;
import net.sourceforge.argparse4j.inf.Namespace;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

import java.util.List;

public class GetRedisCommandStatsCommand extends ConfiguredCommand<WhisperServerConfiguration> {

    public GetRedisCommandStatsCommand() {
        super("rediscommandstats", "Dump Redis command stats");
    }

    @Override
    protected void run(final Bootstrap<WhisperServerConfiguration> bootstrap, final Namespace namespace, final WhisperServerConfiguration config) {
        final ClientResources redisClusterClientResources = ClientResources.builder().build();

        final FaultTolerantRedisCluster cacheCluster         = new FaultTolerantRedisCluster("main_cache_cluster", config.getCacheClusterConfiguration(), redisClusterClientResources);
        final FaultTolerantRedisCluster messagesCacheCluster = new FaultTolerantRedisCluster("messages_cluster", config.getMessageCacheConfiguration().getRedisClusterConfiguration(), redisClusterClientResources);
        final FaultTolerantRedisCluster metricsCluster       = new FaultTolerantRedisCluster("metrics_cluster", config.getMetricsClusterConfiguration(), redisClusterClientResources);

        for (final FaultTolerantRedisCluster cluster : List.of(cacheCluster, messagesCacheCluster, metricsCluster)) {
            cluster.useCluster(connection -> connection.sync()
                    .masters()
                    .commands()
                    .info("commandstats")
                    .asMap()
                    .forEach((node, commandStats) -> {
                        System.out.format("# %s - %s\n\n", cluster.getName(), node.getUri());
                        System.out.println(commandStats);
                    }));
        }
    }
}
