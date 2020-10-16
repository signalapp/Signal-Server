package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

import java.util.List;

public class GetRedisCommandStatsCommand extends ConfiguredCommand<WhisperServerConfiguration> {

    public GetRedisCommandStatsCommand() {
        super("rediscommandstats", "Dump Redis command stats");
    }

    @Override
    protected void run(final Bootstrap<WhisperServerConfiguration> bootstrap, final Namespace namespace, final WhisperServerConfiguration config) throws Exception {
        final FaultTolerantRedisCluster cacheCluster         = new FaultTolerantRedisCluster("main_cache_cluster", config.getCacheClusterConfiguration());
        final FaultTolerantRedisCluster messagesCacheCluster = new FaultTolerantRedisCluster("messages_cluster", config.getMessageCacheConfiguration().getRedisClusterConfiguration());
        final FaultTolerantRedisCluster metricsCluster       = new FaultTolerantRedisCluster("metrics_cluster", config.getMetricsClusterConfiguration());

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
