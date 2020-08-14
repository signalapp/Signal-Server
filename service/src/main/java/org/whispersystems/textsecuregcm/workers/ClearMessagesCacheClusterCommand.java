package org.whispersystems.textsecuregcm.workers;

import io.dropwizard.cli.ConfiguredCommand;
import io.dropwizard.setup.Bootstrap;
import net.sourceforge.argparse4j.inf.Namespace;
import org.whispersystems.textsecuregcm.WhisperServerConfiguration;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;

public class ClearMessagesCacheClusterCommand extends ConfiguredCommand<WhisperServerConfiguration> {

    public ClearMessagesCacheClusterCommand() {
        super("clearmessagescluster", "remove all keys from messages cache cluster");
    }

    @Override
    protected void run(final Bootstrap<WhisperServerConfiguration> bootstrap, final Namespace namespace, final WhisperServerConfiguration config) {
        final FaultTolerantRedisCluster messagesCacheCluster = new FaultTolerantRedisCluster("messages_cluster", config.getMessageCacheConfiguration().getRedisClusterConfiguration().getUrls(), config.getMessageCacheConfiguration().getRedisClusterConfiguration().getTimeout(), config.getMessageCacheConfiguration().getRedisClusterConfiguration().getCircuitBreakerConfiguration());
        messagesCacheCluster.useCluster(connection -> connection.sync().masters().commands().flushallAsync());
    }
}
