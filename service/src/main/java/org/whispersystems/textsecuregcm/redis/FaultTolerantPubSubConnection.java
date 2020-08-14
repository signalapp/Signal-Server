package org.whispersystems.textsecuregcm.redis;

import com.codahale.metrics.SharedMetricRegistries;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.function.Consumer;
import java.util.function.Function;

public class FaultTolerantPubSubConnection<K, V> {

    private final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection;
    private final CircuitBreaker circuitBreaker;

    public FaultTolerantPubSubConnection(final String name, final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection, final CircuitBreakerConfiguration circuitBreakerConfiguration) {
        this.pubSubConnection = pubSubConnection;
        this.circuitBreaker   = CircuitBreaker.of(name + "-pubsub", circuitBreakerConfiguration.toCircuitBreakerConfig());

        CircuitBreakerUtil.registerMetrics(SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME),
                this.circuitBreaker,
                FaultTolerantRedisCluster.class);

        this.pubSubConnection.setNodeMessagePropagation(true);
    }

    public void usePubSubConnection(final Consumer<StatefulRedisClusterPubSubConnection<K, V>> consumer) {
        this.circuitBreaker.executeRunnable(() -> consumer.accept(pubSubConnection));
    }

    public <T> T withPubSubConnection(final Function<StatefulRedisClusterPubSubConnection<K, V>, T> consumer) {
        return this.circuitBreaker.executeSupplier(() -> consumer.apply(pubSubConnection));
    }
}
