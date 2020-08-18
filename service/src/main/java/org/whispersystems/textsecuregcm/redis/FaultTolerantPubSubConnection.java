package org.whispersystems.textsecuregcm.redis;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.lettuce.core.cluster.pubsub.StatefulRedisClusterPubSubConnection;
import org.whispersystems.textsecuregcm.configuration.CircuitBreakerConfiguration;
import org.whispersystems.textsecuregcm.util.CircuitBreakerUtil;
import org.whispersystems.textsecuregcm.util.Constants;

import java.util.function.Consumer;
import java.util.function.Function;

import static com.codahale.metrics.MetricRegistry.name;

public class FaultTolerantPubSubConnection<K, V> {

    private final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection;
    private final CircuitBreaker circuitBreaker;

    private final Timer executeTimer;

    public FaultTolerantPubSubConnection(final String name, final StatefulRedisClusterPubSubConnection<K, V> pubSubConnection, final CircuitBreakerConfiguration circuitBreakerConfiguration) {
        this.pubSubConnection = pubSubConnection;
        this.circuitBreaker   = CircuitBreaker.of(name + "-pubsub", circuitBreakerConfiguration.toCircuitBreakerConfig());

        CircuitBreakerUtil.registerMetrics(SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME),
                this.circuitBreaker,
                FaultTolerantRedisCluster.class);

        this.pubSubConnection.setNodeMessagePropagation(true);

        final MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);

        this.executeTimer = metricRegistry.timer(name(getClass(), name + "-pubsub", "execute"));
    }

    public void usePubSubConnection(final Consumer<StatefulRedisClusterPubSubConnection<K, V>> consumer) {
        try (final Timer.Context ignored = executeTimer.time()) {
            this.circuitBreaker.executeRunnable(() -> consumer.accept(pubSubConnection));
        }
    }

    public <T> T withPubSubConnection(final Function<StatefulRedisClusterPubSubConnection<K, V>, T> consumer) {
        try (final Timer.Context ignored = executeTimer.time()) {
            return this.circuitBreaker.executeSupplier(() -> consumer.apply(pubSubConnection));
        }
    }
}
