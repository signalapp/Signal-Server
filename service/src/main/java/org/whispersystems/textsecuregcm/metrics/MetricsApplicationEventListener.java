/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.metrics;

import org.glassfish.jersey.server.monitoring.ApplicationEvent;
import org.glassfish.jersey.server.monitoring.ApplicationEventListener;
import org.glassfish.jersey.server.monitoring.RequestEvent;
import org.glassfish.jersey.server.monitoring.RequestEventListener;
import org.whispersystems.textsecuregcm.storage.ClientReleaseManager;

/**
 * Delegates request events to a listener that captures and reports request-level metrics.
 *
 * @see MetricsHttpChannelListener
 * @see MetricsRequestEventListener
 */
public class MetricsApplicationEventListener implements ApplicationEventListener {

  private final MetricsRequestEventListener metricsRequestEventListener;

  public MetricsApplicationEventListener(final TrafficSource trafficSource, final ClientReleaseManager clientReleaseManager) {
    if (trafficSource == TrafficSource.HTTP) {
      throw new IllegalArgumentException("Use " + MetricsHttpChannelListener.class.getName() + " for HTTP traffic");
    }
    this.metricsRequestEventListener = new MetricsRequestEventListener(trafficSource, clientReleaseManager);
  }

  @Override
  public void onEvent(final ApplicationEvent event) {
  }

  @Override
  public RequestEventListener onRequest(final RequestEvent requestEvent) {
    return metricsRequestEventListener;
  }
}
