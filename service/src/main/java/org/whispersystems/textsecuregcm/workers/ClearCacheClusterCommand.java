package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

public class ClearCacheClusterCommand extends ConfiguredCommand<WhisperServerConfiguration> {

    public ClearCacheClusterCommand() {
        super("clearcache", "remove all keys from cache cluster");
    }

    @Override
    protected void run(final Bootstrap<WhisperServerConfiguration> bootstrap, final Namespace namespace, final WhisperServerConfiguration config) {
        final FaultTolerantRedisCluster cacheCluster = new FaultTolerantRedisCluster("main_cache_cluster", config.getCacheClusterConfiguration().getUrls(), config.getCacheClusterConfiguration().getTimeout(), config.getCacheClusterConfiguration().getCircuitBreakerConfiguration());
        cacheCluster.useWriteCluster(connection -> connection.sync().flushall());
    }
}
