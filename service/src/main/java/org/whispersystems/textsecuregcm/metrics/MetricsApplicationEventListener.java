package org.whispersystems.textsecuregcm.metrics;

import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;

/**
 * Delegates request events to a listener that captures and reports request-level metrics.
 */
public class MetricsApplicationEventListener implements ApplicationEventListener {

    private final MetricsRequestEventListener metricsRequestEventListener;

    public MetricsApplicationEventListener(final TrafficSource trafficSource) {
        this.metricsRequestEventListener = new MetricsRequestEventListener(trafficSource);
    }

    @Override
    public void onEvent(final ApplicationEvent event) {
    }

    @Override
    public RequestEventListener onRequest(final RequestEvent requestEvent) {
        return metricsRequestEventListener;
    }
}
